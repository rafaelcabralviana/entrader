from __future__ import annotations

import time
from unittest.mock import patch

from django.contrib.auth.models import User
from django.test import Client, RequestFactory, TestCase
from django.contrib.sessions.middleware import SessionMiddleware

from trader import panel_context as pc


def _attach_session(req):
    middleware = SessionMiddleware(lambda r: None)
    middleware.process_request(req)
    req.session.save()
    return req


class DaytradeActiveResolverTests(TestCase):
    def test_resolve_picks_first_non_endofday(self):
        rf = RequestFactory()
        req = _attach_session(rf.get('/'))

        req.session[pc._SESSION_DAYTRADE_WIN_CANDIDATES] = 'WIN_A,WIN_B'
        req.session[pc._SESSION_DAYTRADE_ACTIVE_WIN] = ''
        req.session[pc._SESSION_DAYTRADE_ACTIVE_WIN_AT] = ''

        def fake_fetch_quote(ticker: str, *, use_cache: bool = True):
            if ticker == 'WIN_A':
                return {'status': 'EndOfDay'}
            if ticker == 'WIN_B':
                return {'status': 'Trading'}
            return {'status': 'EndOfDay'}

        with patch('trader.panel_context.fetch_quote', side_effect=fake_fetch_quote):
            out = pc.resolve_daytrade_base_ticker(req, 'WIN')
        self.assertEqual(out, 'WIN_B')

    def test_resolve_force_ignores_cached(self):
        rf = RequestFactory()
        req = _attach_session(rf.get('/'))

        req.session[pc._SESSION_DAYTRADE_WIN_CANDIDATES] = 'WIN_A,WIN_B'
        req.session[pc._SESSION_DAYTRADE_ACTIVE_WIN] = 'WIN_A'
        req.session[pc._SESSION_DAYTRADE_ACTIVE_WIN_AT] = str(time.time())

        def fake_fetch_quote(ticker: str, *, use_cache: bool = True):
            if ticker == 'WIN_A':
                return {'status': 'EndOfDay'}
            if ticker == 'WIN_B':
                return {'status': 'Trading'}
            return {'status': 'EndOfDay'}

        with patch('trader.panel_context.fetch_quote', side_effect=fake_fetch_quote):
            out = pc.resolve_daytrade_base_ticker(req, 'WIN', force=True)
        self.assertEqual(out, 'WIN_B')


class DaytradeCandidatesSaveViewTests(TestCase):
    def test_post_saves_candidates_in_session(self):
        user = User.objects.create_user('u', password='p')
        c = Client()
        c.force_login(user)

        r = c.post(
            '/market/daytrade-candidates-save/',
            {
                'win_candidates': 'WIN_A, WIN_B',
                'wdo_candidates': 'WDO_X WDO_Y',
                'next': '/mercado/',
            },
            follow=False,
        )
        self.assertIn(r.status_code, (302, 303))

        session = c.session
        self.assertEqual(session.get(pc._SESSION_DAYTRADE_WIN_CANDIDATES), 'WIN_A,WIN_B')
        self.assertEqual(session.get(pc._SESSION_DAYTRADE_WDO_CANDIDATES), 'WDO_X,WDO_Y')

