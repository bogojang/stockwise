from __future__ import annotations
from flask import Flask, jsonify, render_template, request, Response, stream_with_context
from dotenv import load_dotenv
import yfinance as yf
import pandas as pd
import json
import os
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging
from datetime import datetime, timedelta

load_dotenv()

app = Flask(__name__)
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
DART_API_KEY = os.getenv('DART_API_KEY', '').strip()

# ── 캐시 ─────────────────────────────────────────────────────────────────────
_cache: dict = {}
_cache_times: dict = {}
CACHE_TTL = 3600  # 1시간

def get_cache(key):
    if key in _cache and time.time() - _cache_times.get(key, 0) < CACHE_TTL:
        return _cache[key]
    return None

def set_cache(key, value):
    _cache[key] = value
    _cache_times[key] = time.time()


# ── 테마 분류 ─────────────────────────────────────────────────────────────────
THEMES = {
    '반도체':    ['반도체', '전자부품', 'D램', '낸드', '파운드리', 'DRAM', 'Semiconductor'],
    '2차전지':   ['배터리', '이차전지', '전기차', '양극재', '음극재', '전해질', 'Battery'],
    'AI·IT':    ['소프트웨어', 'IT서비스', '인터넷', '게임', '클라우드', '데이터', 'Software'],
    '바이오·제약': ['바이오', '제약', '의약품', '의료기기', '헬스', 'Bio', 'Pharma'],
    '자동차':    ['자동차', '자동차부품', '타이어', 'Auto'],
    '금융·보험': ['은행', '증권', '보험', '금융투자', '캐피탈'],
    '건설·부동산': ['건설', '건자재', '부동산'],
    '화학·정유': ['화학', '정유', '합성수지', '도료', '폴리머'],
    '소비·유통': ['유통', '식품', '음료', '의류', '화장품', '뷰티'],
    '조선·기계': ['조선', '기계', '중공업', '플랜트'],
    '철강·소재': ['철강', '금속', '알루미늄', '구리'],
    '통신·미디어': ['통신', '방송', '미디어', '엔터'],
}

def classify_themes(sector: str, industry: str, name: str) -> list:
    text = f"{sector} {industry} {name}"
    result = []
    for theme, keywords in THEMES.items():
        if any(kw in text for kw in keywords):
            result.append(theme)
    return result[:3]


# ── 섹터 한국어 번역 ──────────────────────────────────────────────────────────
SECTOR_KO = {
    'Technology': '기술', 'Healthcare': '헬스케어',
    'Financial Services': '금융서비스', 'Consumer Cyclical': '경기소비재',
    'Consumer Defensive': '필수소비재', 'Industrials': '산업재',
    'Basic Materials': '소재', 'Energy': '에너지',
    'Real Estate': '부동산', 'Utilities': '유틸리티',
    'Communication Services': '통신서비스',
}

def translate_sector(s: str) -> str:
    return SECTOR_KO.get(s or '', s or '-')


# ── 점수 계산 ─────────────────────────────────────────────────────────────────
def calculate_score(per, pbr, roe, debt_ratio) -> dict:
    if per and per > 0:
        if 5 <= per <= 12:  ps = 25
        elif per < 5:       ps = 15
        elif per <= 20:     ps = 18
        elif per <= 30:     ps = 10
        elif per <= 50:     ps = 5
        else:               ps = 2
    else:
        ps = 0

    if pbr and pbr > 0:
        if pbr <= 1.0:   bs = 25
        elif pbr <= 1.5: bs = 22
        elif pbr <= 2.0: bs = 17
        elif pbr <= 3.0: bs = 10
        else:            bs = 4
    else:
        bs = 0

    if roe is not None:
        if roe >= 25:   rs = 25
        elif roe >= 20: rs = 22
        elif roe >= 15: rs = 18
        elif roe >= 10: rs = 13
        elif roe >= 5:  rs = 7
        elif roe >= 0:  rs = 3
        else:           rs = 0
    else:
        rs = 0

    if debt_ratio is not None and debt_ratio >= 0:
        if debt_ratio <= 30:    ds = 25
        elif debt_ratio <= 60:  ds = 22
        elif debt_ratio <= 100: ds = 17
        elif debt_ratio <= 150: ds = 12
        elif debt_ratio <= 200: ds = 7
        else:                   ds = 2
    else:
        ds = 10

    total = ps + bs + rs + ds
    grade = 'BUY' if total >= 75 else 'HOLD' if total >= 55 else 'WATCH' if total >= 35 else 'AVOID'

    return {
        'total': total, 'max': 100, 'grade': grade,
        'breakdown': {
            'per':  {'score': ps, 'max': 25, 'value': per},
            'pbr':  {'score': bs, 'max': 25, 'value': pbr},
            'roe':  {'score': rs, 'max': 25, 'value': roe},
            'debt': {'score': ds, 'max': 25, 'value': debt_ratio},
        }
    }


# ── 재무 트렌드 분석 (3~4년) ──────────────────────────────────────────────────
def get_financial_trend(ticker: str) -> dict | None:
    try:
        obj = yf.Ticker(ticker)
        fin = obj.financials  # rows=지표, columns=날짜(최신순)
        if fin is None or fin.empty:
            return None

        cols = sorted(fin.columns, reverse=True)[:4]  # 최신 4개 연도
        year_labels, revenues, op_profits = [], [], []

        for col in cols:
            year_labels.append(str(col.year) if hasattr(col, 'year') else str(col)[:4])

            rev = None
            for k in ['Total Revenue', 'Revenue']:
                if k in fin.index:
                    v = fin.loc[k, col]
                    if v is not None and not pd.isna(v):
                        rev = float(v); break
            revenues.append(rev)

            op = None
            for k in ['Operating Income', 'Ebit', 'Operating Revenue']:
                if k in fin.index:
                    v = fin.loc[k, col]
                    if v is not None and not pd.isna(v):
                        op = float(v); break
            op_profits.append(op)

        # 오래된 순으로 뒤집기
        return {
            'year_labels': year_labels[::-1],
            'revenues':    revenues[::-1],
            'op_profits':  op_profits[::-1],
        }
    except Exception as e:
        logger.debug(f"[trend] {ticker}: {e}")
        return None


def consecutive_growth(values: list) -> int:
    """오래된→최신 순 리스트에서 연속 증가 연수 반환"""
    valid = [v for v in values if v is not None and v > 0]
    if len(valid) < 2:
        return 0
    count = 0
    for i in range(len(valid) - 1, 0, -1):
        if valid[i] > valid[i - 1]:
            count += 1
        else:
            break
    return count


# ── 시그널 생성 ───────────────────────────────────────────────────────────────
def consecutive_decline(values: list) -> int:
    """오래된→최신 순 리스트에서 연속 감소 연수 반환"""
    valid = [v for v in values if v is not None]
    if len(valid) < 2:
        return 0
    count = 0
    for i in range(len(valid) - 1, 0, -1):
        if valid[i] < valid[i - 1]:
            count += 1
        else:
            break
    return count


def generate_signals(data: dict, sector_avg: dict = None) -> list:
    pos_signals, warn_signals = [], []
    trend = data.get('financial_trend') or {}
    revenues  = trend.get('revenues', [])
    op_profits = trend.get('op_profits', [])

    # ── 긍정 시그널 ──────────────────────────────────────────────────────────
    rev_cons = consecutive_growth(revenues)
    if rev_cons >= 3:
        pos_signals.append({'type': 'pos', 'icon': '📈', 'text': f'최근 {rev_cons}년 연속 매출액 증가'})
    elif rev_cons == 2:
        pos_signals.append({'type': 'pos', 'icon': '📈', 'text': '최근 2년 연속 매출액 증가'})

    op_cons = consecutive_growth(op_profits)
    if op_cons >= 3:
        pos_signals.append({'type': 'pos', 'icon': '💰', 'text': f'최근 {op_cons}년 연속 영업이익 증가'})
    elif op_cons == 2:
        pos_signals.append({'type': 'pos', 'icon': '💰', 'text': '최근 2년 연속 영업이익 증가'})

    pbr = data.get('pbr')
    if pbr and 0 < pbr < 1.0:
        pos_signals.append({'type': 'pos', 'icon': '💎', 'text': f'PBR {pbr:.2f}배 — 청산 가치 이하 저평가'})

    roe = data.get('roe')
    if roe and roe >= 20:
        pos_signals.append({'type': 'pos', 'icon': '🏆', 'text': f'ROE {roe:.1f}% — 우수한 자본 효율성'})

    div = data.get('dividend_yield')
    if div and div >= 0.03:
        pos_signals.append({'type': 'pos', 'icon': '💵', 'text': f'배당수익률 {div*100:.1f}% — 고배당주'})

    debt = data.get('debt_ratio')
    if debt is not None and debt < 30:
        pos_signals.append({'type': 'pos', 'icon': '🛡️', 'text': f'부채비율 {debt:.0f}% — 재무 안정성 우수'})

    price  = data.get('current_price')
    low52  = data.get('week52_low')
    high52 = data.get('week52_high')
    pos52  = None
    if price and low52 and high52 and (high52 - low52) > 0:
        pos52 = (price - low52) / (high52 - low52)
        if pos52 < 0.2:
            pos_signals.append({'type': 'pos', 'icon': '🎯', 'text': '52주 최저가 근처 — 저점 매수 기회'})

    if sector_avg:
        per = data.get('per')
        avg_per = sector_avg.get('avg_per')
        if per and avg_per and per > 0 and avg_per > 0:
            ratio = per / avg_per
            if ratio <= 0.75:
                pos_signals.append({'type': 'pos', 'icon': '⭐',
                    'text': f'업종 내 저평가 (업종 PER {avg_per:.1f}배 대비 {int((1-ratio)*100)}% 낮음)'})
        if roe and sector_avg.get('avg_roe') and roe > sector_avg['avg_roe'] * 1.3:
            pos_signals.append({'type': 'pos', 'icon': '🏆',
                'text': f'업종 평균 ROE({sector_avg["avg_roe"]:.1f}%) 대비 우수'})

    # ── 경고 시그널 (매도 주의) ───────────────────────────────────────────────
    # 매출액 연속 감소
    rev_dec = consecutive_decline(revenues)
    if rev_dec >= 2:
        warn_signals.append({'type': 'warn', 'icon': '📉',
            'text': f'최근 {rev_dec}년 연속 매출액 감소 — 성장성 악화'})

    # 영업이익 연속 감소 또는 적자
    op_dec = consecutive_decline(op_profits)
    if op_dec >= 2:
        warn_signals.append({'type': 'warn', 'icon': '🔻',
            'text': f'최근 {op_dec}년 연속 영업이익 감소 — 수익성 악화'})

    valid_op = [v for v in op_profits if v is not None]
    neg_op = sum(1 for v in valid_op if v < 0)
    if neg_op >= 2:
        warn_signals.append({'type': 'warn', 'icon': '🚨',
            'text': f'최근 {neg_op}년 영업 적자 지속 — 본업 경쟁력 우려'})
    elif neg_op == 1 and valid_op and valid_op[-1] < 0:
        warn_signals.append({'type': 'warn', 'icon': '🚨', 'text': '최근 영업 적자 전환 — 수익성 악화'})

    # ROE 마이너스 (순손실)
    if roe is not None and roe < 0:
        warn_signals.append({'type': 'warn', 'icon': '💸',
            'text': f'ROE {roe:.1f}% — 순손실 (자본 잠식 위험)'})

    # 부채비율 과다
    if debt is not None:
        if debt > 300:
            warn_signals.append({'type': 'warn', 'icon': '⛔',
                'text': f'부채비율 {debt:.0f}% — 매우 높은 재무 위험'})
        elif debt > 200:
            warn_signals.append({'type': 'warn', 'icon': '⚠️',
                'text': f'부채비율 {debt:.0f}% — 재무 안정성 취약'})

    # PER 과도한 고평가
    per = data.get('per')
    if per and per > 60:
        warn_signals.append({'type': 'warn', 'icon': '💣',
            'text': f'PER {per:.1f}배 — 과도한 고평가, 조정 위험'})

    # 업종 대비 고평가
    if sector_avg:
        avg_per = sector_avg.get('avg_per')
        if per and avg_per and per > 0 and avg_per > 0:
            ratio = per / avg_per
            if ratio >= 2.0:
                warn_signals.append({'type': 'warn', 'icon': '⚠️',
                    'text': f'업종 평균 PER({avg_per:.1f}배) 대비 2배 이상 고평가'})
            elif ratio >= 1.5:
                warn_signals.append({'type': 'warn', 'icon': '⚠️',
                    'text': f'업종 평균 대비 PER 고평가 구간'})

    # 52주 최고가 근처
    if pos52 is not None and pos52 > 0.9:
        warn_signals.append({'type': 'warn', 'icon': '📛',
            'text': '52주 최고가 근처 — 추격 매수 위험'})

    # PBR 고평가
    if pbr and pbr > 5:
        warn_signals.append({'type': 'warn', 'icon': '⚠️',
            'text': f'PBR {pbr:.1f}배 — 자산 대비 매우 고평가'})

    # 시그널 조합: 긍정/경고 각각 최대 2개, 합계 4개
    combined = pos_signals[:2] + warn_signals[:2]
    if len(combined) < 4:
        combined += pos_signals[2:4-len(combined)]
    return combined[:4]


# ── yfinance 기본 데이터 ──────────────────────────────────────────────────────
def fetch_stock_data(ticker: str) -> dict | None:
    try:
        obj = yf.Ticker(ticker)
        info = obj.info
        if not info or (not info.get('regularMarketPrice') and not info.get('currentPrice')):
            return None

        per = info.get('trailingPE') or info.get('forwardPE')
        pbr = info.get('priceToBook')
        roe_raw = info.get('returnOnEquity')
        roe = round(roe_raw * 100, 2) if roe_raw is not None else None
        dte = info.get('debtToEquity')
        debt_ratio = round(dte, 2) if dte is not None else None
        rg = info.get('revenueGrowth')
        om = info.get('operatingMargins')
        div = info.get('dividendYield')

        sector_raw = info.get('sector', '')
        sector = translate_sector(sector_raw) if sector_raw else '-'

        return {
            'per':              round(per, 2) if per else None,
            'pbr':              round(pbr, 2) if pbr else None,
            'roe':              roe,
            'debt_ratio':       debt_ratio,
            'revenue_growth':   round(rg * 100, 2) if rg is not None else None,
            'operating_margin': round(om * 100, 2) if om is not None else None,
            'dividend_yield':   div,
            'market_cap':       info.get('marketCap'),
            'current_price':    info.get('currentPrice') or info.get('regularMarketPrice'),
            'currency':         info.get('currency', 'KRW'),
            'sector':           sector,
            'industry':         info.get('industry', '-'),
            'long_name':        info.get('longName') or info.get('shortName', ''),
            'total_revenue':    info.get('totalRevenue'),
            'net_income':       info.get('netIncomeToCommon'),
            'week52_high':      info.get('fiftyTwoWeekHigh'),
            'week52_low':       info.get('fiftyTwoWeekLow'),
        }
    except Exception as e:
        logger.debug(f"[fetch] {ticker}: {e}")
        return None


def fetch_price_history(ticker: str) -> list:
    try:
        hist = yf.Ticker(ticker).history(period='1y')
        if hist.empty:
            return []
        hist = hist.reset_index()
        return [{'date': str(r['Date'])[:10], 'close': round(float(r['Close']), 2)}
                for _, r in hist.iterrows()]
    except:
        return []


# ── 내장 종목 목록 (FinanceDataReader 없이 동작) ─────────────────────────────
# (코드, 한국어 이름, 섹터, 업종)
_KOSPI_RAW = [
    ('005930','삼성전자',       '기술',     '반도체'),
    ('000660','SK하이닉스',     '기술',     '반도체'),
    ('207940','삼성바이오로직스','헬스케어', '바이오'),
    ('005380','현대차',         '경기소비재','자동차'),
    ('000270','기아',           '경기소비재','자동차'),
    ('051910','LG화학',         '소재',     '화학'),
    ('006400','삼성SDI',        '소재',     '2차전지'),
    ('028260','삼성물산',        '산업재',  '건설'),
    ('035420','NAVER',          '통신서비스','IT서비스'),
    ('105560','KB금융',         '금융서비스','은행'),
    ('055550','신한지주',        '금융서비스','은행'),
    ('032830','삼성생명',        '금융서비스','보험'),
    ('012330','현대모비스',      '경기소비재','자동차부품'),
    ('066570','LG전자',         '경기소비재','전자'),
    ('003550','LG',             '산업재',   '지주사'),
    ('017670','SK텔레콤',       '통신서비스','통신'),
    ('030200','KT',             '통신서비스','통신'),
    ('015760','한국전력',        '유틸리티', '전력'),
    ('096770','SK이노베이션',    '에너지',  '정유'),
    ('034730','SK',             '산업재',   '지주사'),
    ('009150','삼성전기',        '기술',    '전자부품'),
    ('000810','삼성화재',        '금융서비스','보험'),
    ('086790','하나금융지주',    '금융서비스','은행'),
    ('138040','메리츠금융지주',  '금융서비스','금융'),
    ('035720','카카오',          '통신서비스','IT서비스'),
    ('316140','우리금융지주',    '금융서비스','은행'),
    ('003490','대한항공',        '산업재',  '항공'),
    ('267250','HD현대',          '산업재',  '지주사'),
    ('005490','POSCO홀딩스',     '소재',    '철강'),
    ('047050','포스코인터내셔널','산업재',  '유통'),
    ('003670','포스코퓨처엠',    '소재',    '2차전지'),
    ('024110','IBK기업은행',     '금융서비스','은행'),
    ('018260','삼성SDS',         '기술',    '소프트웨어'),
    ('011200','HMM',             '산업재',  '해운'),
    ('010130','고려아연',        '소재',    '금속'),
    ('259960','크래프톤',        '통신서비스','게임'),
    ('032640','LG유플러스',      '통신서비스','통신'),
    ('000100','유한양행',        '헬스케어', '제약'),
    ('051900','LG생활건강',      '필수소비재','화장품'),
    ('090430','아모레퍼시픽',    '필수소비재','화장품'),
    ('004020','현대제철',        '소재',    '철강'),
    ('000720','현대건설',        '산업재',  '건설'),
    ('097950','CJ제일제당',      '필수소비재','식품'),
    ('010950','S-Oil',           '에너지',  '정유'),
    ('078930','GS',              '산업재',  '지주사'),
    ('010620','HD현대미포',      '산업재',  '조선'),
    ('042660','한화오션',        '산업재',  '조선'),
    ('012450','한화에어로스페이스','산업재','방산'),
    ('009540','HD한국조선해양',  '산업재',  '조선'),
    ('006800','미래에셋증권',    '금융서비스','증권'),
    ('064350','현대로템',        '산업재',  '방산'),
    ('047810','한국항공우주',    '산업재',  '방산'),
    ('161390','한국타이어앤테크놀로지','경기소비재','타이어'),
    ('021240','코웨이',          '경기소비재','가전'),
    ('011780','금호석유',        '소재',    '화학'),
    ('071050','한국금융지주',    '금융서비스','증권'),
    ('006360','GS건설',         '산업재',  '건설'),
    ('011170','롯데케미칼',      '소재',    '화학'),
]

_KOSDAQ_RAW = [
    ('247540','에코프로비엠',    '소재',     '2차전지'),
    ('086520','에코프로',        '소재',     '2차전지'),
    ('196170','알테오젠',        '헬스케어', '바이오'),
    ('042700','한미반도체',      '기술',     '반도체'),
    ('357780','솔브레인',        '소재',     '반도체'),
    ('066970','L&F',            '소재',     '2차전지'),
    ('293490','카카오게임즈',    '통신서비스','게임'),
    ('035900','JYP엔터테인먼트','통신서비스','엔터'),
    ('122870','와이지엔터테인먼트','통신서비스','엔터'),
    ('041510','SM엔터테인먼트', '통신서비스','엔터'),
    ('214150','클래시스',        '헬스케어', '의료기기'),
    ('145020','휴젤',            '헬스케어', '바이오'),
    ('058470','리노공업',        '기술',     '반도체'),
    ('317000','에코프로에이치엔','소재',     '2차전지'),
    ('039030','이오테크닉스',    '기술',     '반도체'),
    ('263750','펄어비스',        '통신서비스','게임'),
    ('323410','카카오뱅크',      '금융서비스','은행'),
    ('277810','레인보우로보틱스','기술',     'AI로봇'),
    ('096530','씨젠',            '헬스케어', '바이오'),
    ('403870','HPSP',           '기술',     '반도체'),
    ('091990','셀트리온헬스케어','헬스케어', '바이오'),
    ('112040','위메이드',        '통신서비스','게임'),
    ('064760','티씨케이',        '기술',     '반도체'),
    ('018290','브이티',          '헬스케어', '화장품'),
    ('214450','파마리서치',      '헬스케어', '바이오'),
    ('009420','한올바이오파마',  '헬스케어', '제약'),
    ('036830','솔브레인홀딩스',  '소재',     '반도체'),
    ('101400','비나텍',          '소재',     '2차전지'),
    ('036540','SFA반도체',       '기술',     '반도체'),
    ('000990','DB하이텍',        '기술',     '반도체'),
    ('054620','APS홀딩스',       '기술',     '반도체'),
    ('232140','와이아이케이',    '기술',     '반도체'),
    ('048410','현대바이오',      '헬스케어', '바이오'),
    ('025900','동화기업',        '소재',     '2차전지'),
    ('256940','케이피엠테크',    '기술',     '반도체'),
]


def _listing_from_pykrx(market: str) -> list[dict]:
    """KRX에서 실시간 시총 상위 종목 목록 가져오기"""
    from pykrx import stock as pykrx_stock

    today = datetime.now()
    # 주말이면 직전 금요일로
    while today.weekday() >= 5:
        today -= timedelta(days=1)
    date_str = today.strftime('%Y%m%d')

    limit = 80 if market == 'KOSPI' else 60
    suffix = '.KS' if market == 'KOSPI' else '.KQ'

    df = pykrx_stock.get_market_cap_by_ticker(date_str, market=market)
    if df is None or df.empty:
        raise ValueError('pykrx 데이터 없음')

    df = df.sort_values('시가총액', ascending=False).head(limit)

    result = []
    for ticker in df.index:
        try:
            name = pykrx_stock.get_market_ticker_name(ticker)
        except Exception:
            name = ticker
        result.append({
            'ticker':   f"{ticker}{suffix}",
            'kr_code':  ticker,
            'name':     name,
            'sector':   '-',
            'industry': '-',
        })
    return result


def get_listing(market: str) -> list[dict]:
    """pykrx(실시간) → 실패 시 하드코딩 폴백"""
    try:
        result = _listing_from_pykrx(market)
        logger.info(f"[pykrx] {market} {len(result)}개 로드")
        return result
    except Exception as e:
        logger.warning(f"[pykrx 실패, 폴백 사용] {e}")
        suffix = '.KS' if market == 'KOSPI' else '.KQ'
        raw = _KOSPI_RAW if market == 'KOSPI' else _KOSDAQ_RAW
        return [
            {'ticker': f"{code}{suffix}", 'kr_code': code,
             'name': name, 'sector': sector, 'industry': industry}
            for code, name, sector, industry in raw
        ]


# ── 종목 결과 조립 ────────────────────────────────────────────────────────────
def build_result(meta: dict, market: str, sector_avg: dict = None) -> dict | None:
    ticker = meta['ticker']
    data   = fetch_stock_data(ticker)
    if not data:
        return None

    trend = get_financial_trend(ticker)

    # FDR의 한국어 섹터 우선, yfinance 보완
    sector   = meta.get('sector') or data.get('sector') or '-'
    industry = meta.get('industry') or data.get('industry') or '-'
    name     = meta.get('name') or data.get('long_name') or ticker

    themes  = classify_themes(sector, industry, name)
    scoring = calculate_score(data.get('per'), data.get('pbr'), data.get('roe'), data.get('debt_ratio'))

    full = {**data, 'financial_trend': trend}
    signals = generate_signals(full, sector_avg)

    return {
        'ticker':           ticker,
        'kr_code':          meta.get('kr_code', ''),
        'name':             name,
        'market':           market,
        'sector':           sector,
        'industry':         industry,
        'themes':           themes,
        'per':              data.get('per'),
        'pbr':              data.get('pbr'),
        'roe':              data.get('roe'),
        'debt_ratio':       data.get('debt_ratio'),
        'revenue_growth':   data.get('revenue_growth'),
        'operating_margin': data.get('operating_margin'),
        'dividend_yield':   data.get('dividend_yield'),
        'current_price':    data.get('current_price'),
        'currency':         data.get('currency'),
        'market_cap':       data.get('market_cap'),
        'week52_high':      data.get('week52_high'),
        'week52_low':       data.get('week52_low'),
        'financial_trend':  trend,
        'signals':          signals,
        'scoring':          scoring,
    }


# ── 업종 평균 계산 ────────────────────────────────────────────────────────────
def calc_sector_averages(stocks: list) -> dict:
    """섹터별 PER/PBR/ROE 평균 계산"""
    from collections import defaultdict
    buckets = defaultdict(list)
    for s in stocks:
        sec = s.get('sector', '-')
        buckets[sec].append(s)

    result = {}
    for sec, items in buckets.items():
        pers  = [s['per']  for s in items if s.get('per')  and s['per'] > 0]
        pbrs  = [s['pbr']  for s in items if s.get('pbr')  and s['pbr'] > 0]
        roes  = [s['roe']  for s in items if s.get('roe')  is not None]
        result[sec] = {
            'avg_per': round(sum(pers)/len(pers), 2) if pers else None,
            'avg_pbr': round(sum(pbrs)/len(pbrs), 2) if pbrs else None,
            'avg_roe': round(sum(roes)/len(roes), 2) if roes else None,
            'count':   len(items),
        }
    return result


# ── 직접 종목 검색 ────────────────────────────────────────────────────────────
@app.route('/api/search-stock')
def api_search_stock():
    query = request.args.get('q', '').strip()
    market = request.args.get('market', 'KOSPI').upper()
    if not query or len(query) < 1:
        return jsonify({'status': 'ok', 'data': []})

    cached = get_cache(f'market_{market}')
    if cached:
        q = query.upper()
        matched = [s for s in cached if
                   q in s['ticker'].upper() or
                   q in s['name'].upper() or
                   q in s.get('kr_code', '')]
        return jsonify({'status': 'ok', 'data': matched[:10]})

    # 캐시 없으면 실시간 단건 조회
    suffix = '.KS' if market == 'KOSPI' else '.KQ'
    ticker = f"{query.zfill(6)}{suffix}" if query.isdigit() else query
    data = fetch_stock_data(ticker)
    if data:
        return jsonify({'status': 'ok', 'data': [{'ticker': ticker, 'name': data.get('long_name', ticker), **data}]})
    return jsonify({'status': 'ok', 'data': []})


# ── API 라우트 ────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/cache/clear')
def api_cache_clear():
    _cache.clear(); _cache_times.clear()
    return jsonify({'status': 'ok'})


# ── SSE 스트리밍 ──────────────────────────────────────────────────────────────
@app.route('/api/market/<market>/stream')
def api_market_stream(market: str):
    market = market.upper()
    if market not in ('KOSPI', 'KOSDAQ'):
        return jsonify({'error': '지원하지 않는 시장'}), 400

    cached = get_cache(f'market_{market}')

    def generate():
        # ── 캐시 적중 ──
        if cached is not None:
            sector_avgs = calc_sector_averages(cached)
            for i, stock in enumerate(cached):
                # 캐시된 데이터에 업종 평균 시그널 재생성
                sec_avg = sector_avgs.get(stock.get('sector', '-'))
                full = {**stock}
                stock['signals'] = generate_signals(full, sec_avg)
                yield f"data: {json.dumps({'type':'stock','data':stock,'scanned':i+1,'total':len(cached),'cached':True})}\n\n"
            yield f"data: {json.dumps({'type':'done','total':len(cached),'sector_avgs':sector_avgs,'cached':True})}\n\n"
            return

        # ── 새 스캔 ──
        try:
            listing = get_listing(market)
        except Exception as e:
            yield f"data: {json.dumps({'type':'error','message':str(e)})}\n\n"
            return

        total = len(listing)
        result_queue = queue.Queue()
        all_results = []

        def worker(meta):
            res = build_result(meta, market)
            result_queue.put(res)

        executor = ThreadPoolExecutor(max_workers=8)
        for meta in listing:
            executor.submit(worker, meta)
        executor.shutdown(wait=False)

        scanned = 0
        while scanned < total:
            try:
                res = result_queue.get(timeout=90)
                scanned += 1
                if res:
                    all_results.append(res)
                    yield f"data: {json.dumps({'type':'stock','data':res,'scanned':scanned,'total':total})}\n\n"
                else:
                    yield f"data: {json.dumps({'type':'progress','scanned':scanned,'total':total})}\n\n"
            except queue.Empty:
                break

        # 업종 평균 계산 후 최종 시그널 업데이트
        sector_avgs = calc_sector_averages(all_results)
        for s in all_results:
            sec_avg = sector_avgs.get(s.get('sector', '-'))
            s['signals'] = generate_signals(s, sec_avg)

        all_results.sort(key=lambda x: x['scoring']['total'], reverse=True)
        set_cache(f'market_{market}', all_results)

        buy  = sum(1 for r in all_results if r['scoring']['grade'] == 'BUY')
        hold = sum(1 for r in all_results if r['scoring']['grade'] == 'HOLD')
        yield f"data: {json.dumps({'type':'done','total':len(all_results),'scanned':scanned,'buy':buy,'hold':hold,'sector_avgs':sector_avgs})}\n\n"

    return Response(
        stream_with_context(generate()),
        content_type='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
    )


# ── 종목 상세 ─────────────────────────────────────────────────────────────────
@app.route('/api/stock/<path:ticker>')
def api_stock(ticker: str):
    cached = get_cache(f'detail_{ticker}')
    if cached:
        return jsonify({'status': 'ok', 'data': cached})

    data = fetch_stock_data(ticker)
    if not data:
        return jsonify({'status': 'error', 'message': '종목을 찾을 수 없습니다'}), 404

    trend   = get_financial_trend(ticker)
    scoring = calculate_score(data.get('per'), data.get('pbr'), data.get('roe'), data.get('debt_ratio'))
    history = fetch_price_history(ticker)
    full    = {**data, 'financial_trend': trend}
    signals = generate_signals(full)

    result = {'ticker': ticker, **data, 'financial_trend': trend,
              'scoring': scoring, 'history': history, 'signals': signals}
    set_cache(f'detail_{ticker}', result)
    return jsonify({'status': 'ok', 'data': result})


if __name__ == '__main__':
    app.run(debug=True, port=5000, threaded=True)
