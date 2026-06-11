#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
STOCKSONAR 파이프라인 v2
GlobeNewswire RSS 수집 → 필터링 → yfinance 시세 → Claude AI 분석 → docs/data.json 갱신 (+텔레그램 알림)

v2 개선:
- 티커가 여러 개 감지되면 Claude가 '뉴스의 실제 주체' 티커를 직접 선택 (귀속 오류 방지)
- 일정 공지·웹캐스트·컨퍼런스 참가 등 저가치 뉴스는 AI 분석 전에 차단 (비용 절약)
- 같은 종목의 유사 제목 재발행(재탕) 중복 제거

환경변수:
  ANTHROPIC_API_KEY   (필수) Claude API 키
  MODEL               (선택) 기본 claude-haiku-4-5-20251001
  ALERT_MIN_SCORE     (선택) 텔레그램 알림 기준 점수, 기본 70
  TELEGRAM_BOT_TOKEN  (선택) 텔레그램 봇 토큰
  TELEGRAM_CHAT_ID    (선택) 텔레그램 채팅/채널 ID
  MAX_NEW_PER_RUN     (선택) 1회 실행당 최대 분석 건수(비용 가드), 기본 20
  SONAR_MOCK          (선택) 1이면 네트워크/API 없이 모의 데이터로 전체 흐름 테스트
"""
import os, re, json, hashlib, time, html, sys
from datetime import datetime, timezone, timedelta

MOCK = os.environ.get("SONAR_MOCK") == "1"

RSS_URL = "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies"
DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "docs", "data.json")
MODEL = os.environ.get("MODEL", "claude-haiku-4-5-20251001")
ALERT_MIN_SCORE = int(os.environ.get("ALERT_MIN_SCORE", "70"))
MAX_NEW_PER_RUN = int(os.environ.get("MAX_NEW_PER_RUN", "20"))
KEEP_DAYS = 30          # 뉴스 보관 기간
MAX_ITEMS = 400         # data.json 최대 보관 건수
MAX_SEEN = 4000         # 중복 방지 해시 보관 수

# ── 미국 정규 거래소만 허용 (비상장/OTC 제외) ─────────────────────────────
EXCH_MAP = {"NMS": "NASDAQ", "NGM": "NASDAQ", "NCM": "NASDAQ",
            "NYQ": "NYSE", "ASE": "NYSE AM", "PCX": "NYSE ARCA"}

# ── 스팸: 법률 소송·주주 소송 권유 등 (제목+본문 검사) ────────────────────
SPAM_KEYWORDS = [
    "class action", "investigation", "law firm", "lawsuit", "shareholder alert",
    "investor alert", "lead plaintiff", "deadline reminder", "securities fraud",
    "rosen law", "pomerantz", "glancy", "bronstein", "kahn swick", "levi & korsinsky",
    "schall law", "gross law", "kessler topaz", "hagens berman", "faruqi",
    "robbins geller", "johnson fistel", "wolf haldenstein", "kirby mcinerney",
    "total voting rights", "annual general meeting",
]

# ── 저가치: 일정·행사 공지 등 (제목만 검사, AI 분석 전 차단 → 비용 절약) ──
LOW_VALUE_KEYWORDS = [
    "to release", "to report", "to announce", "to present at", "to participate in",
    "to attend", "to host", "to webcast", "fireside chat", "investor conference",
    "conference call", "earnings call", "earnings date", "results date",
    "financial results on", "reporting date", "to ring the", "market open bell",
    "invitation to", "reminder:", "save the date", "files annual report",
    "form 10-k", "form 20-f", "form 6-k", "annual report on form",
    "monthly distribution", "declares monthly", "net asset value",
    "closed-end fund", "completion of depositary",
]

TICKER_RE = re.compile(
    r"\(\s*(?:NASDAQ|Nasdaq|NYSE\s*American|NYSE\s*Amer\.?|NYSE|NYSEAMERICAN|AMEX|CBOE)\s*[:\-]\s*([A-Z]{1,5})(?:\.[A-Z])?\s*\)"
)


def log(msg):
    print(f"[sonar] {msg}", flush=True)


def now_utc():
    return datetime.now(timezone.utc)


def h(s):
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]


def norm_title_key(tk, title):
    """재탕 방지용: 종목 + 제목 핵심 단어로 해시 (대소문자·문장부호·어순 일부 무시)"""
    words = re.findall(r"[a-z0-9]+", title.lower())
    return h(tk + ":" + " ".join(sorted(set(words))[:12]))


# ════════════════════════════════ 1. 데이터 로드/저장 ════════════════════
def load_data():
    try:
        with open(DATA_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"updated": None, "fx": 1450.0, "items": [], "seen": []}


def save_data(data):
    os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)
    with open(DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    log(f"data.json 저장 완료 — {len(data['items'])}건")


# ════════════════════════════════ 2. RSS 수집 ════════════════════════════
MOCK_ENTRIES = [
    {"title": "Acme Quantum (NASDAQ: ACMQ) Announces $25 Million AI Data Center Contract With Sovereign Fund",
     "summary": "Acme Quantum, a low float technology company, announced a binding agreement...",
     "link": "https://example.com/acmq", "published": now_utc().isoformat()},
    {"title": "Rosen Law Firm Reminds Investors of Class Action Against BigCo (NYSE: BIGC)",
     "summary": "lead plaintiff deadline...", "link": "https://example.com/spam",
     "published": now_utc().isoformat()},
    {"title": "Beta Bio (NASDAQ: BTBO) Prices $50 Million Public Offering of Common Stock",
     "summary": "Beta Bio announced the pricing of its underwritten public offering...",
     "link": "https://example.com/bbio", "published": now_utc().isoformat()},
    {"title": "Private Startup Announces New Product Line",
     "summary": "no ticker here", "link": "https://example.com/notk",
     "published": now_utc().isoformat()},
    # v2 테스트: 다중 티커 — 발표 주체(NDAQ)가 아니라 대상 기업(INHD)에 귀속되어야 함
    {"title": "Nasdaq Halts Inno Holdings Inc.",
     "summary": "The Nasdaq Stock Market LLC (Nasdaq: NDAQ) announced that trading in Inno Holdings Inc. (NASDAQ: INHD) was halted pending additional information.",
     "link": "https://example.com/halt", "published": now_utc().isoformat()},
    # v2 테스트: 저가치 일정 공지 — AI 분석 없이 차단되어야 함
    {"title": "Gamma Corp (NASDAQ: GMMA) to Report Second Quarter 2026 Financial Results",
     "summary": "Gamma Corp will host a conference call and webcast...",
     "link": "https://example.com/cal", "published": now_utc().isoformat()},
    # v2 테스트: 재탕 — ACMQ와 동일 제목 어순만 다름
    {"title": "Acme Quantum Announces $25 Million AI Data Center Contract With Sovereign Fund (NASDAQ: ACMQ)",
     "summary": "duplicate rewrite...", "link": "https://example.com/acmq2",
     "published": now_utc().isoformat()},
]


def fetch_rss():
    if MOCK:
        log("MOCK: RSS 모의 데이터 사용")
        return MOCK_ENTRIES
    import feedparser
    feed = feedparser.parse(RSS_URL, agent="Mozilla/5.0 (StockSonar; +personal project)")
    out = []
    for e in feed.entries:
        pub = None
        if getattr(e, "published_parsed", None):
            pub = datetime(*e.published_parsed[:6], tzinfo=timezone.utc).isoformat()
        out.append({"title": e.get("title", ""), "summary": e.get("summary", ""),
                    "link": e.get("link", ""), "published": pub or now_utc().isoformat()})
    log(f"RSS {len(out)}건 수신")
    return out


def extract_tickers(entry):
    """제목 → 본문 순으로 모든 티커 후보를 추출 (순서 유지, 중복 제거, 최대 3개)"""
    cands = []
    for text in (entry["title"], entry["summary"]):
        for m in TICKER_RE.finditer(html.unescape(text or "")):
            t = m.group(1)
            if t not in cands:
                cands.append(t)
    return cands[:3]


def is_spam(entry):
    t = (entry["title"] + " " + entry["summary"]).lower()
    return any(k in t for k in SPAM_KEYWORDS)


def is_low_value(entry):
    t = entry["title"].lower()
    return any(k in t for k in LOW_VALUE_KEYWORDS)


# ════════════════════════════════ 3. 시세 (yfinance) ═════════════════════
def get_quote(tk):
    if MOCK:
        return {"price": 3.21, "chg": 12.5, "mcapM": 45.0, "sharesM": 12.0,
                "exch": "NASDAQ", "name": f"{tk} Inc (mock)"}
    import yfinance as yf
    try:
        info = yf.Ticker(tk).info
        exch = EXCH_MAP.get(info.get("exchange", ""))
        if not exch:
            return None  # OTC/비상장 등 제외
        price = info.get("regularMarketPrice") or info.get("currentPrice")
        if not price:
            return None
        chg = info.get("regularMarketChangePercent")
        mcap = info.get("marketCap") or 0
        flt = info.get("floatShares") or info.get("sharesOutstanding") or 0
        return {"price": round(float(price), 4),
                "chg": round(float(chg), 2) if chg is not None else None,
                "mcapM": round(mcap / 1e6, 2),
                "sharesM": round(flt / 1e6, 2),
                "exch": exch,
                "name": info.get("shortName") or info.get("longName") or tk}
    except Exception as ex:
        log(f"yfinance 실패 {tk}: {ex}")
        return None


def get_fx(prev):
    if MOCK:
        return 1450.0
    import yfinance as yf
    try:
        v = yf.Ticker("USDKRW=X").info.get("regularMarketPrice")
        return round(float(v), 2) if v else prev
    except Exception:
        return prev


# ════════════════════════════════ 4. Claude AI 분석 ══════════════════════
SYSTEM_PROMPT = """너는 미국 주식 단기 트레이딩 뉴스 분석가다. 영문 보도자료를 분석해 아래 JSON만 출력한다(설명 금지).

{"tk":"뉴스의 실제 주체(대상 기업) 티커 — 반드시 주어진 후보 중에서 선택",
 "ko":"한국어 제목(간결, 핵심 수치 포함)",
 "sum":"한국어 요약 2~3문장. 무엇이 발표됐고 왜 주가에 중요한지.",
 "sent":"good|bad|neut",
 "score":0~100 정수,
 "tags":[["+요인","p"] 또는 ["−요인","m"] 형식 3~5개],
 "cmt":"트레이더 관점 코멘트 1~2문장. 기회와 리스크를 균형있게."}

tk 선택 규칙: 발표 '주체'가 거래소·지수사업자·대형 파트너(예: Nasdaq, NYSE, S&P)여도,
뉴스로 인해 주가가 움직일 '대상 기업'의 티커를 선택하라.
예: "Nasdaq Halts XYZ" → XYZ. "Microsoft Partners With SmallCo" → SmallCo(후보에 있다면).

score(SONAR 점수) 산정 기준:
- 촉매 강도(최대 40): 인수합병·대형계약·FDA승인 35~40 / 정부프로그램·파트너십 25~35 / 임상데이터 20~30 / 학회발표·전임상 15~25 / 홍보성PR 5~15
- 수급 구조(최대 30): 유통주식 5M 미만 25~30 / 25M 미만 15~25 / 100M 미만 5~15 / 그 이상 0~5
- 가격대(최대 15): $0.5~$10 사이 10~15 / $10~$30 5~10 / 그 외 0~5
- 모멘텀(최대 15): 당일 +10% 이상 10~15 / 상승 중 5~10 / 하락 중 0~5
- 페널티: 신주발행·전환사채·희석 −20~−40 / 소송·규제·거래중단 −20~−30 / 구체성 없는 PR −10~−20
악재(공모·희석·소송·거래중단 등)는 sent="bad"로, 점수는 낮게. 판단 불가·홍보성은 "neut"."""


def analyze(entry, candidates):
    """candidates: [(tk, quote), ...] — Claude가 주체 티커를 선택해 분석"""
    if MOCK:
        title = entry["title"]
        pick = next((t for t, q in candidates if t in title), candidates[0][0])
        if "INHD" in entry["summary"]:
            pick = "INHD"
        bad = "offering" in title.lower() or "halt" in title.lower()
        return {"tk": pick, "ko": f"[모의] {pick} 분석 제목", "sum": "모의 요약입니다.",
                "sent": "bad" if bad else "good", "score": 25 if bad else 80,
                "tags": [["+모의 태그", "p"], ["−모의 리스크", "m"]], "cmt": "모의 코멘트."}
    from anthropic import Anthropic
    client = Anthropic()
    cand_lines = "\n".join(
        f"- {t}: 주가 ${q['price']}, 당일등락 {q.get('chg')}%, 시총 {q['mcapM']}M달러, "
        f"유통주식 {q['sharesM']}M주, 거래소 {q['exch']}" for t, q in candidates)
    ctx = (f"제목: {entry['title']}\n본문 요약: {entry['summary'][:1500]}\n\n"
           f"티커 후보:\n{cand_lines}")
    msg = client.messages.create(model=MODEL, max_tokens=800, temperature=0.2,
                                 system=SYSTEM_PROMPT,
                                 messages=[{"role": "user", "content": ctx}])
    text = msg.content[0].text
    m = re.search(r"\{.*\}", text, re.S)
    out = json.loads(m.group(0))
    cand_tks = [t for t, _ in candidates]
    if out.get("tk") not in cand_tks:
        out["tk"] = cand_tks[0]
    out["score"] = max(0, min(100, int(out.get("score", 50))))
    if out.get("sent") not in ("good", "bad", "neut"):
        out["sent"] = "neut"
    out["tags"] = [[str(t[0])[:30], "m" if (len(t) > 1 and t[1] == "m") else "p"]
                   for t in (out.get("tags") or [])][:5]
    return out


# ════════════════════════════════ 5. 텔레그램 알림 ═══════════════════════
def telegram_alert(item):
    tok, chat = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if not tok or not chat:
        return
    if MOCK:
        log(f"MOCK: 텔레그램 알림 — {item['tk']} {item['score']}점")
        return
    import requests
    q = item["q"]
    text = (f"📡 <b>SONAR {item['score']}</b> — <b>${item['tk']}</b> "
            f"(${q['price']}, {'+' if (q.get('chg') or 0) >= 0 else ''}{q.get('chg')}%)\n"
            f"{item['ko']}\n💡 {item['cmt']}\n{item.get('url','')}")
    try:
        requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                      json={"chat_id": chat, "text": text, "parse_mode": "HTML",
                            "disable_web_page_preview": True}, timeout=10)
    except Exception as ex:
        log(f"텔레그램 실패: {ex}")


# ════════════════════════════════ 메인 ═══════════════════════════════════
def main():
    data = load_data()
    seen = set(data.get("seen", []))
    entries = fetch_rss()

    new_items, analyzed = [], 0
    for e in entries:
        key = h(e["link"] or e["title"])
        if key in seen:
            continue
        seen.add(key)                      # 본 뉴스는 결과와 무관하게 기록(재시도 낭비 방지)
        if is_spam(e):
            log(f"스팸 제외: {e['title'][:60]}")
            continue
        if is_low_value(e):
            log(f"저가치 제외(일정/공시 공지): {e['title'][:60]}")
            continue
        cands = extract_tickers(e)
        if not cands:
            continue
        # 재탕 방지: 첫 후보 기준 제목 유사 해시
        tkey = norm_title_key(cands[0], e["title"])
        if tkey in seen:
            log(f"재탕 제외: {e['title'][:60]}")
            continue
        seen.add(tkey)
        if analyzed >= MAX_NEW_PER_RUN:
            log("이번 실행 분석 한도 도달 — 다음 실행에서 계속")
            break
        # 후보별 시세 (정규 거래소만 통과)
        cand_quotes = []
        for t in cands:
            q = get_quote(t)
            if q:
                cand_quotes.append((t, q))
        if not cand_quotes:
            log(f"시세 없음/비정규거래소 제외: {','.join(cands)}")
            continue
        try:
            ai = analyze(e, cand_quotes)
        except Exception as ex:
            log(f"AI 분석 실패 {cands[0]}: {ex}")
            continue
        analyzed += 1
        tk = ai.pop("tk")
        quote = dict(next(q for t, q in cand_quotes if t == tk))
        item = {"tk": tk, "name": quote.pop("name", tk), "time": e["published"],
                "url": e["link"], "en": e["title"], "q": quote, **ai}
        new_items.append(item)
        log(f"분석 완료: {tk} score={ai['score']} {ai['sent']}"
            + (f" (후보 {len(cand_quotes)}개 중 선택)" if len(cand_quotes) > 1 else ""))
        if ai["score"] >= ALERT_MIN_SCORE and ai["sent"] == "good":
            telegram_alert(item)
        time.sleep(0.5)                    # API 예의상 간격

    # 병합: 새 항목 앞에, 오래된 항목 제거
    cutoff = (now_utc() - timedelta(days=KEEP_DAYS)).isoformat()
    items = new_items + [i for i in data["items"] if i.get("time", "") >= cutoff]
    data["items"] = items[:MAX_ITEMS]
    data["seen"] = list(seen)[-MAX_SEEN:]
    data["fx"] = get_fx(data.get("fx", 1450.0))
    data["updated"] = now_utc().isoformat()
    save_data(data)
    log(f"완료 — 신규 {len(new_items)}건 / 전체 {len(data['items'])}건")


if __name__ == "__main__":
    main()
