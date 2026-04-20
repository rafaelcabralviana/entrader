"""
Gera cotações simuladas para testes de automação / replay / candles (pregão **09:00–18:00 BRT**).

**Não execute este arquivo com** ``python simular_cotacoes_dia.py`` **(ModuleNotFoundError:
trader).** Use sempre na raiz do projeto: ``python manage.py simular_cotacoes_dia ...``.

Cada ``quote_data`` segue o mesmo conjunto de chaves que um quote real (ex. BBDC4)::

    {"ticker":"BBDC4","lastPrice":21.22,"lastQuantity":200,"tradeDateTime":"…754Z","status":"EndOfDay",
     "endAuctionTime":null,"open":21.3,"high":21.59,"low":21.13,"close":21.26,
     "previousClosing":{"price":21.26,"date":"2026-04-17"},"dateTime":"…035Z"}

Nada além disso (sem ``Ticker`` duplicado). Timestamps em UTC com ms. Ações: ``previousClosing.date``
em ``YYYY-MM-DD``; BMF: ``date`` null.

Padrão: curva tipo **pregão com alta e baixa** (senoides + ruído) e volume oscilando; use ``--aleatorio``
para o antigo random walk. Com ``--intervalo`` curto (ex.: 1 s), o ruído por tick é **reduzido** (√intervalo/30 s)
para os candles agregados não ficarem com pavios artificiais. ``captured_at`` = instante em **America/Sao_Paulo**
(mesmo dia ``--data``).
O gráfico de candles agrupa por ``captured_at`` (BRT) = dia ``--data``; não pelo ``dateTime`` do JSON.

Exemplos::

    python manage.py simular_cotacoes_dia --ticker PETR4 --data 2026-04-10
    python manage.py simular_cotacoes_dia --ticker PETR4 --data 2026-04-10 --substituir
    python manage.py simular_cotacoes_dia --data 2026-04-10 --intervalo 15 --semente 42
    python manage.py simular_cotacoes_dia --ticker PETR4 --data 2026-04-18 --intervalo 1 --substituir
    python manage.py simular_cotacoes_dia --data 2026-04-10 --exportar /tmp/petr4_sim.json --sem-banco
    python manage.py simular_cotacoes_dia --ticker PETR4 --data 2026-04-10 --apenas-limpar
    python manage.py simular_cotacoes_dia --ticker PETR4 --data 2026-04-10 --apenas-limpar --livro
"""

from __future__ import annotations

import json
import math
import random
from argparse import ArgumentParser
from datetime import date, datetime, time as time_cls, timedelta, timezone as py_timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from zoneinfo import ZoneInfo

from trader.models import BookSnapshot, QuoteSnapshot
from trader.services.quote_history import compute_quote_latency_ms, _parse_quote_event_datetime
from trader.smart_trader_limits import extract_bmf_base

_TZ_SP = ZoneInfo('America/Sao_Paulo')

_SESSION_OPEN = time_cls(9, 0, 0)
_SESSION_CLOSE = time_cls(18, 0, 0)

_MSG_GRAFICO = (
    'Gráfico de candles: no painel escolha o dia = %s (BRT, igual a --data). '
    'O agrupamento usa o campo captured_at de cada linha, não só o dateTime do JSON.'
)


def _notice_grafico_brt(cmd: BaseCommand, session_day: date) -> None:
    cmd.stdout.write(cmd.style.NOTICE(_MSG_GRAFICO % session_day.isoformat()))


def _q2(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))


def _is_bmf(sym: str) -> bool:
    return extract_bmf_base(sym) is not None


def _fmt_price(sym: str, value: float) -> float | int:
    if _is_bmf(sym):
        return int(round(value))
    return _q2(value)


def _sp_aware(dt: datetime) -> datetime:
    if timezone.is_naive(dt):
        return timezone.make_aware(dt, _TZ_SP)
    return dt.astimezone(_TZ_SP)


def _utc_z_ms(dt_sp: datetime, *, extra_ms: int = 0) -> str:
    """
    Mesmo estilo dos quotes reais (ITUB4/WINM26): ``2026-04-17T21:39:58.759Z``.
    """
    u = _sp_aware(dt_sp).astimezone(py_timezone.utc) + timedelta(milliseconds=int(extra_ms))
    u = u.replace(microsecond=random.randint(0, 999_999))
    frac_ms = min(999, u.microsecond // 1000)
    return u.strftime('%Y-%m-%dT%H:%M:%S') + f'.{frac_ms:03d}Z'


def _trade_and_quote_datetimes(event_dt: datetime) -> tuple[str, str]:
    """(tradeDateTime, dateTime) em Z, com defasagem leve como nos snapshots reais."""
    base_ms = random.randint(0, 600)
    lag_ms = random.randint(12, 280)
    return _utc_z_ms(event_dt, extra_ms=base_ms), _utc_z_ms(event_dt, extra_ms=base_ms + lag_ms)


def _previous_closing_real_shape(sym: str, session_day: date, prior_close_price: float | int) -> dict[str, Any]:
    """
    WINM26 real: ``{price, date: null}`` sem ``dateTime`` interno.
    ITUB4 real: ``{price, date: 'YYYY-MM-DD'}`` sem ``dateTime`` interno.
    """
    if _is_bmf(sym):
        return {'price': prior_close_price, 'date': None}
    prev_cal = session_day - timedelta(days=1)
    return {'price': prior_close_price, 'date': prev_cal.isoformat()}


def _build_quote_dict(
    sym: str,
    event_dt: datetime,
    last: float,
    day_open: float,
    day_high: float,
    day_low: float,
    *,
    previous_closing: dict[str, Any],
    status: str,
    close_override: float | int | None,
    quantity: int | None = None,
) -> dict[str, Any]:
    event_dt = _sp_aware(event_dt)
    trade_z, quote_z = _trade_and_quote_datetimes(event_dt)
    lp = _fmt_price(sym, last)
    o = _fmt_price(sym, day_open)
    h = _fmt_price(sym, day_high)
    low = _fmt_price(sym, day_low)
    if close_override is not None:
        cls = _fmt_price(sym, float(close_override))
    else:
        cls = lp
    if quantity is not None:
        qty = int(quantity)
    elif _is_bmf(sym):
        qty = random.randint(1, 25)
    else:
        qty = random.randint(50, 8000)
    return {
        'ticker': sym,
        'lastPrice': lp,
        'lastQuantity': qty,
        'tradeDateTime': trade_z,
        'status': status,
        'endAuctionTime': None,
        'open': o,
        'high': h,
        'low': low,
        'close': cls,
        'previousClosing': previous_closing,
        'dateTime': quote_z,
    }


class Command(BaseCommand):
    help = 'Gera snapshots simulados (09–18h BRT): curva com alta/baixa e volume; --aleatorio = random walk antigo.'

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument('--ticker', default='PETR4', help='Ticker (default PETR4).')
        parser.add_argument('--data', default='2026-04-10', help='Dia ISO (YYYY-MM-DD).')
        parser.add_argument(
            '--intervalo',
            type=int,
            default=30,
            help='Segundos entre cada snapshot (default 30). Com pregão 9–18h BRT, 1 s ≈ 32,4k linhas/dia.',
        )
        parser.add_argument(
            '--preco-inicial',
            type=float,
            default=33.85,
            help='Preço inicial do random walk (default 33.85).',
        )
        parser.add_argument(
            '--volatilidade',
            type=float,
            default=0.04,
            help=(
                'Amplitude base do ruído por tick (ações: R$; BMF: pontos aprox.). '
                'Com --intervalo < 30 s o comando escala para baixo (√intervalo/30) para suavizar candles.'
            ),
        )
        parser.add_argument(
            '--amplitude-pct',
            type=float,
            default=4.5,
            help='Modo onda (default): oscilação ~%% do preço central (default 4.5).',
        )
        parser.add_argument(
            '--aleatorio',
            action='store_true',
            help='Random walk antigo em vez da curva com alta/baixa (onda + volume).',
        )
        parser.add_argument(
            '--semente',
            type=int,
            default=None,
            help='Seed do RNG para reprodutibilidade.',
        )
        parser.add_argument(
            '--substituir',
            action='store_true',
            help='Remove snapshots existentes do ticker nesse dia antes de inserir.',
        )
        parser.add_argument(
            '--exportar',
            default='',
            help='Caminho de arquivo JSON (lista de {captured_at, quote_data}).',
        )
        parser.add_argument(
            '--sem-banco',
            action='store_true',
            help='Não grava no banco (útil com --exportar).',
        )
        parser.add_argument(
            '--apenas-limpar',
            action='store_true',
            help='Só apaga QuoteSnapshot do ticker no dia (BRT); não gera nem insere nada.',
        )
        parser.add_argument(
            '--livro',
            action='store_true',
            help='Com --apenas-limpar: apaga também BookSnapshot do mesmo ticker e dia.',
        )
        parser.add_argument(
            '--status',
            default='Trading',
            choices=['Trading', 'EndOfDay'],
            help='Valor do campo status (default Trading).',
        )
        parser.add_argument(
            '--ultimo-eod',
            action='store_true',
            help='Só no último ponto: status EndOfDay e close levemente diferente de lastPrice (ex.: BBDC4).',
        )

    def handle(self, *args: Any, **options: Any) -> None:
        sym = (options['ticker'] or '').strip().upper()
        if not sym:
            raise CommandError('Ticker vazio.')

        raw_day = options['data']
        try:
            session_day = date.fromisoformat(str(raw_day).strip())
        except ValueError as e:
            raise CommandError(f'Data inválida: {raw_day!r}') from e

        day_start = datetime.combine(session_day, time_cls.min, tzinfo=_TZ_SP)
        day_end = day_start + timedelta(days=1)
        t_open = datetime.combine(session_day, _SESSION_OPEN, tzinfo=_TZ_SP)
        t_close = datetime.combine(session_day, _SESSION_CLOSE, tzinfo=_TZ_SP)

        if options['apenas_limpar']:
            nq, _ = QuoteSnapshot.objects.filter(
                ticker__iexact=sym,
                captured_at__gte=day_start,
                captured_at__lt=day_end,
            ).delete()
            self.stdout.write(f'QuoteSnapshot removidos: {nq} ({sym} em {session_day.isoformat()}, BRT).')
            if options['livro']:
                nb, _ = BookSnapshot.objects.filter(
                    ticker__iexact=sym,
                    captured_at__gte=day_start,
                    captured_at__lt=day_end,
                ).delete()
                self.stdout.write(f'BookSnapshot removidos: {nb}.')
            return

        interval = int(options['intervalo'])
        if interval < 1:
            raise CommandError('--intervalo deve ser >= 1.')

        seed = options['semente']
        if seed is not None:
            random.seed(seed)

        vol = float(options['volatilidade'])
        use_wave = not bool(options.get('aleatorio'))
        amp_pct = float(options.get('amplitude_pct') or 4.5) / 100.0
        duration_sec = max(1.0, (t_close - t_open).total_seconds())
        bmf = _is_bmf(sym)

        # lastPrice entra nos candles agregados; ruído i.i.d. forte a cada tick com intervalo 1 s
        # infla artificialmente high−low. Referência 30 s (default histórico): escala √(interval/30).
        tick_ref_sec = 30.0
        noise_scale = min(1.0, (max(interval, 1) / tick_ref_sec) ** 0.5)

        center0 = float(options['preco_inicial'])
        if bmf:
            center0 = float(int(round(center0)))
            noise_span = max(vol, 80.0) * noise_scale
        else:
            center0 = _q2(center0)
            noise_span = vol * noise_scale

        if interval < int(tick_ref_sec):
            self.stdout.write(
                self.style.NOTICE(
                    f'Ruído por tick ×{noise_scale:.3f} (intervalo {interval}s vs ref. {tick_ref_sec:.0f}s) — candles mais lisos.'
                )
            )

        price = center0
        prior_raw = center0 - (200.0 if bmf else 0.12)
        prior_close_px = _fmt_price(sym, prior_raw)
        prev_block = _previous_closing_real_shape(sym, session_day, prior_close_px)

        times: list[datetime] = []
        cur = t_open
        while cur <= t_close:
            times.append(cur)
            cur += timedelta(seconds=interval)

        base_status = str(options.get('status') or 'Trading')
        ultimo_eod = bool(options.get('ultimo_eod'))

        rows_out: list[dict[str, Any]] = []
        batch: list[QuoteSnapshot] = []

        day_open_val: float | int | None = None
        day_high: float | int = center0
        day_low: float | int = center0

        shock_state = 0.0
        vol_state = 1.0
        drift_state = 0.0

        for idx, cur in enumerate(times):
            frac = (cur - t_open).total_seconds() / duration_sec
            qty: int | None

            if use_wave:
                # Curva-base intraday + ruído estocástico com memória curta.
                # Evita sequência artificial de candles "com o mesmo passo".
                macro = math.sin(frac * math.pi) * center0 * amp_pct
                ripple = math.sin(frac * 7.0 * math.pi) * center0 * (amp_pct * 0.28)
                dip = math.cos(frac * 3.0 * math.pi) * center0 * (amp_pct * 0.12)
                target = center0 + macro + ripple + dip
                vol_state = (vol_state * random.uniform(0.82, 0.97)) + random.uniform(0.04, 0.24)
                drift_state = (drift_state * random.uniform(0.55, 0.9)) + random.uniform(
                    -0.18 * noise_span, 0.18 * noise_span
                )
                local_vol = (
                    0.35
                    + abs(math.sin(frac * 5.0 * math.pi)) * 0.75
                    + vol_state * random.uniform(0.3, 1.65)
                    + random.uniform(-0.16, 0.28)
                )
                local_vol = max(0.25, local_vol)
                local_span = noise_span * local_vol
                mean_revert = (float(target) - float(price)) * random.uniform(0.035, 0.14)
                shock_state = (shock_state * random.uniform(0.35, 0.82)) + random.uniform(
                    -local_span, local_span
                )
                step = mean_revert + shock_state + drift_state
                if random.random() < 0.018:
                    step += random.uniform(-2.4, 2.4) * local_span
                if random.random() < 0.05:
                    step += random.uniform(-0.65, 0.65) * local_span
                if bmf:
                    raw = float(price) + step
                    price = max(1.0, int(round(raw)))
                else:
                    raw = float(price) + step
                    price = max(0.01, _q2(raw))
                vol_pulse = abs(math.sin(frac * 5.0 * math.pi))
                if bmf:
                    qty = max(1, int(2 + vol_pulse * 12 + abs(step) * 0.05 + random.randint(0, 7)))
                else:
                    qty = int(900 + vol_pulse * 9000 + abs(step) * 6800 + random.randint(0, 5400))
            else:
                step = (
                    random.uniform(-noise_span, noise_span)
                    if not bmf
                    else random.randint(
                        -int(max(noise_span, 1)), int(max(noise_span, 1))
                    )
                )
                if bmf:
                    price = max(1.0, float(price) + float(step))
                    price = float(int(round(price)))
                else:
                    price = max(0.01, float(price) + float(step))
                    price = _q2(price)
                qty = None

            if day_open_val is None:
                day_open_val = price
                day_high = day_low = price
            else:
                day_high = max(day_high, price)
                day_low = min(day_low, price)

            is_last = idx == len(times) - 1
            row_status = 'EndOfDay' if (ultimo_eod and is_last) else base_status
            close_ov: float | int | None = None
            if ultimo_eod and is_last:
                if bmf:
                    jitter = random.randint(-25, 25)
                    close_ov = int(max(int(day_low), min(int(day_high), int(round(price)) + jitter)))
                else:
                    close_ov = price
                    if _q2(day_high) > _q2(day_low):
                        for _ in range(10):
                            cand = _q2(price + random.uniform(-0.12, 0.12))
                            cand = max(_q2(day_low), min(_q2(day_high), cand))
                            if abs(cand - price) >= 0.009:
                                close_ov = cand
                                break
                        else:
                            close_ov = max(_q2(day_low), min(_q2(day_high), _q2(price + 0.04)))
                    else:
                        close_ov = _q2(price)

            qd = _build_quote_dict(
                sym,
                cur,
                price,
                day_open_val,
                day_high,
                day_low,
                previous_closing=prev_block,
                status=row_status,
                close_override=close_ov,
                quantity=qty,
            )
            event_dt = cur
            if timezone.is_naive(event_dt):
                event_dt = timezone.make_aware(event_dt, _TZ_SP)
            qe = _parse_quote_event_datetime(qd) or event_dt
            lat = compute_quote_latency_ms(qd)

            rows_out.append(
                {
                    'captured_at': event_dt.isoformat(timespec='seconds'),
                    'quote_data': qd,
                }
            )
            batch.append(
                QuoteSnapshot(
                    ticker=sym,
                    captured_at=event_dt,
                    quote_data=qd,
                    quote_event_at=qe,
                    latency_ms=lat,
                )
            )

        export_path = (options.get('exportar') or '').strip()
        if export_path:
            path = Path(export_path)
            path.write_text(json.dumps(rows_out, ensure_ascii=False, indent=2), encoding='utf-8')
            self.stdout.write(self.style.SUCCESS(f'Exportado: {path} ({len(rows_out)} pontos).'))

        if options['sem_banco']:
            self.stdout.write(
                self.style.WARNING(f'Modo --sem-banco: {len(rows_out)} pontos gerados, sem INSERT.')
            )
            if rows_out:
                _notice_grafico_brt(self, session_day)
            return

        if options['substituir']:
            deleted, _ = QuoteSnapshot.objects.filter(
                ticker__iexact=sym,
                captured_at__gte=day_start,
                captured_at__lt=day_end,
            ).delete()
            self.stdout.write(f'Removidos {deleted} objetos (incl. dependentes) do dia.')

        QuoteSnapshot.objects.bulk_create(batch, batch_size=500)
        self.stdout.write(
            self.style.SUCCESS(
                f'Inseridos {len(batch)} QuoteSnapshot para {sym} em {session_day.isoformat()}.'
            )
        )
        _notice_grafico_brt(self, session_day)
