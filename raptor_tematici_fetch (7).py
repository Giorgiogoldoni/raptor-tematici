#!/usr/bin/env python3
"""
🦅 RAPTOR TEMATICI v2 — BUY1/2/3 + EXIT1/2/3 + DOWNGRADE + stress + events
KAMA(5) · ER · Vortex · RVI · 13 gruppi · max 3 pos/gruppo
"""
import json, os
from datetime import datetime, date, timedelta
import pytz, yfinance as yf, pandas as pd

# TradingView rating — opzionale, non blocca se non disponibile
try:
    from tradingview_ta import TA_Handler, Interval as TV_Interval
    TV_AVAILABLE = True
except ImportError:
    TV_AVAILABLE = False

# Mappa exchange per tradingview_ta
TV_EXCHANGE_MAP = {
    ".MI": ("MIL", "europe"),
    ".DE": ("XETR", "europe"),
    ".PA": ("EURONEXT", "europe"),
    ".L":  ("LSE", "uk"),
    "":    ("NASDAQ", "america"),  # fallback per ticker senza suffisso (es. ICLN)
}

def get_tv_rating(ticker):
    """Recupera rating TradingView per un ticker. Ritorna dict o None."""
    if not TV_AVAILABLE:
        return None
    try:
        # Determina exchange e screener
        suffix = ""
        for sfx in TV_EXCHANGE_MAP:
            if sfx and ticker.endswith(sfx):
                suffix = sfx; break
        exchange, screener = TV_EXCHANGE_MAP[suffix]
        symbol = ticker.replace(suffix, "") if suffix else ticker

        handler = TA_Handler(
            symbol=symbol,
            exchange=exchange,
            screener=screener,
            interval=TV_Interval.INTERVAL_1_DAY,
            timeout=8
        )
        analysis = handler.get_analysis()
        rec = analysis.summary.get("RECOMMENDATION", "NEUTRAL")
        buy = analysis.summary.get("BUY", 0)
        sell = analysis.summary.get("SELL", 0)
        neu  = analysis.summary.get("NEUTRAL", 0)
        return {"tv_rating": rec, "tv_buy": buy, "tv_sell": sell, "tv_neutral": neu}
    except Exception:
        return None

ROME_TZ = pytz.timezone("Europe/Rome")
MAX_POS = 3; KAMA_N = 5; KAMA_FAST = 2; KAMA_SLOW = 20
TARGET_PCT = 7.0; TRAIL_MULT = 1.5; COOLDOWN_DAYS = 2

LEVEL_ORDER = {"BUY1":1,"BUY2":2,"BUY3":3,
               "EXIT1":0,"EXIT1b":0,"EXIT2":0,"EXIT3":0,
               "DOWNGRADE_BUY2":0,"DOWNGRADE_BUY1":0}
SIZE_MAP = {"BUY1":35,"BUY2":70,"BUY3":100,
            "EXIT1":40,"EXIT1b":100,"EXIT2":75,"EXIT3":100,
            "DOWNGRADE_BUY2":40,"DOWNGRADE_BUY1":75}

GROUPS = {
    "ai_tech":{"name":"🤖 AI & TECH","color":"#0969da","tickers":{
        "AIAA.MI":"WisdomTree AI & Tech ETF","AIAI.MI":"L&G Artificial Intelligence ETF",
        "AI4UJ.MI":"iShares Automation & Robotics","AINF.MI":"iShares AI Infrastructure UCITS ETF",
        "AIQE.MI":"iShares AI Equity ETF","GOAI.MI":"Invesco MSCI World ESG UCITS ETF",
        "WTAI.MI":"WisdomTree AI ETF","JEDI.MI":"Amundi MSCI AI ETF",
        "XSGI.MI":"Xtrackers AI & Big Data ETF","GDIG.MI":"iShares Digital Security ETF",
        "DGTL.MI":"Rize Digital Economy ETF","CLOU.MI":"WisdomTree Cloud Computing ETF",
        "CTEK.MI":"Amundi PEA Digital Economy","XCTE.MI":"Xtrackers Cybersecurity ETF",
        "XDER.MI":"Xtrackers Digital Enablers ETF","FAMTEL.MI":"Fineco AM MSCI World IT Sustainable UCITS ETF"}},
    "difesa":{"name":"🛡️ DIFESA & SICUREZZA","color":"#cf222e","tickers":{
        "ARMI.MI":"VanEck Defense ETF","ARMR.MI":"Amundi Future of Defence ETF",
        "DFND.MI":"Rize Defence Innovation ETF","DFNS.MI":"HANetf Future of Defence ETF",
        "WDEF.MI":"WisdomTree Defence ETF","LOCK.MI":"iShares Aerospace & Defence ETF",
        "BUG.MI":"Global X Cybersecurity ETF","CYBO.MI":"Ossiam Cyber Security ETF",
        "WCBR.MI":"WisdomTree Cybersecurity ETF","VPN.MI":"Global X Data Center ETF",
        "ICBR.MI":"iShares Cyber Security & Tech UCITS ETF",
        "FAMMAI.MI":"Fineco AM MSCI ACWI Cyber Security UCITS ETF","ISPY.MI":"L&G Cyber Security UCITS ETF"}},
    "energia":{"name":"⚡ ENERGIA & RINNOVABILI","color":"#e3b341","tickers":{
        "RENW.MI":"HANetf S&P Global Clean Energy","RNRG.MI":"Rize Sustainable Future Energy",
        "REUS.MI":"Amundi MSCI New Energy ETF","SOLR.MI":"Global X Solar Energy ETF",
        "HDRO.MI":"HANetf Hydrogen Economy ETF","HTWO.MI":"L&G Hydrogen Economy ETF",
        "HYGN.MI":"VanEck Hydrogen Economy ETF","VOLT.MI":"Amundi MSCI New Energy Transition",
        "ELCR.MI":"iShares Global Clean Energy ETF","WNDE.MI":"WisdomTree New Energy ETF",
        "WNDY.MI":"WisdomTree Wind Energy ETF","WRNW.MI":"WisdomTree Renewable Energy ETF",
        "NRJC.MI":"Amundi Energy Transition ETF","EEA.MI":"iShares MSCI Europe ESG ETF",
        "GCLE.MI":"iShares Global Clean Energy UCITS","NCLR.MI":"VanEck Uranium & Nuclear ETF",
        "NUCL.MI":"Sprott Uranium Miners ETF","U3O8.MI":"Global X Uranium ETF",
        "URNJ.MI":"Sprott Junior Uranium Miners ETF","OIH.MI":"VanEck Oil Services ETF",
        "XNGI.MI":"Xtrackers MSCI World Energy ETF","ICLN":"iShares Global Clean Energy USD"}},
    "salute":{"name":"💊 SALUTE & BIOTECH","color":"#1a7f37","tickers":{
        "HEAL.MI":"iShares Healthcare Innovation ETF","BIOT.MI":"iShares Nasdaq Biotechnology",
        "CURE.MI":"Global X Genomics & Biotechnology","DOCT.MI":"Amundi MSCI Healthcare ETF",
        "GNOM.MI":"iShares Genomics Immunology ETF","WDNA.MI":"WisdomTree Biorevolution ETF",
        "GENDEE.MI":"Rize Medical Technology ETF","EDOC.MI":"Global X Telemedicine ETF",
        "AGED.MI":"iShares Ageing Population ETF","XGEN.MI":"Xtrackers MSCI World Health",
        "XNNV.MI":"Xtrackers Artificial Intelligence","LABL.MI":"WisdomTree Food Innovation ETF"}},
    "crypto":{"name":"🔗 BLOCKCHAIN & CRYPTO","color":"#f0883e","tickers":{
        "BKCH.MI":"Global X Blockchain ETF","BTC.MI":"ETC Group Physical Bitcoin",
        "DAPP.MI":"VanEck Crypto & Blockchain Innovators","WEB3.MI":"Bitwise Web3 ETF",
        "BTECH.MI":"iShares Blockchain Technology ETF","BTECJ.MI":"iShares Blockchain JPY Hedged",
        "BLTH.MI":"Invesco CoinShares Blockchain ETF"}},
    "mobilita":{"name":"🚗 MOBILITA & EV","color":"#bc4c00","tickers":{
        "ECAR.MI":"iShares Electric Vehicles ETF","EMOVE.MI":"Amundi Future Mobility ETF",
        "EMOVJ.MI":"Amundi Future Mobility JPY Hedged","DRVE.MI":"Rize Electric Vehicle ETF",
        "AUCO.MI":"L&G Eaton Vance Automation ETF","BATT.MI":"Global X Lithium & Battery Tech",
        "LITU.MI":"WisdomTree Battery Solutions ETF","LITM.MI":"iShares Lithium & Battery ETF"}},
    "infrastrutture":{"name":"🏗️ INFRASTRUTTURE & REAL ESTATE","color":"#57606a","tickers":{
        "INFR.MI":"iShares Global Infrastructure ETF","PAVE.MI":"Global X US Infrastructure ETF",
        "EPRA.MI":"iShares Developed Markets Property Yield","EPRE.MI":"SPDR Global Real Estate ETF",
        "SCITY.MI":"iShares Smart City Infrastructure","CITY.MI":"Amundi Smart City ETF",
        "CIT.MI":"Global X Smart City ETF","CITE.MI":"iShares Digitalisation ETF",
        "XLPE.MI":"SPDR S&P US Consumer Discretionary","XRES.MI":"Xtrackers MSCI World Real Estate"}},
    "risorse":{"name":"🌾 RISORSE & MATERIE PRIME","color":"#7d4e00","tickers":{
        "COPM.MI":"iShares Copper Producers ETF","COPR.MI":"Global X Copper Miners ETF",
        "COPX.MI":"Global X Copper ETF","CROP.MI":"iShares Agribusiness ETF",
        "GDX.MI":"VanEck Gold Miners ETF","GDXJ.MI":"VanEck Junior Gold Miners ETF",
        "REMX.MI":"VanEck Rare Earth & Strategic Metals","RARE.MI":"VanEck Rare Earth ETF",
        "METL.MI":"WisdomTree Industrial Metals ETF","METAA.MI":"Amundi MSCI Metals & Mining",
        "METAJ.MI":"Amundi MSCI Metals & Mining JPY","METE.MI":"iShares MSCI Global Metals",
        "SILV.MI":"iShares Silver ETF","WSLV.MI":"WisdomTree Physical Silver ETF",
        "VEGI.MI":"iShares Agribusiness ETF EUR","FOFD.MI":"Rize Food Revolution ETF",
        "GSM.MI":"Global X Silver Miners ETF","FAMAMW.MI":"Fineco AM MSCI World Metals & Mining UCITS ETF"}},
    "digitale":{"name":"📱 DIGITALE & ECOMMERCE","color":"#6e40c9","tickers":{
        "EBIZ.MI":"Global X E-commerce ETF","EBUY.MI":"Amundi MSCI E-commerce ETF",
        "ECOM.MI":"L&G Ecommerce Logistics UCITS ETF","DPAY.MI":"Amundi Digital Economy ETF",
        "FINX.MI":"Global X FinTech ETF","INQQ.MI":"iShares EM Digital Economy ETF",
        "SNSR.MI":"Global X Internet of Things ETF","XFNT.MI":"Xtrackers FinTech Innovation",
        "XMOV.MI":"Xtrackers Future Mobility ETF","XDG3.MI":"Xtrackers Digital Disruption",
        "XDG6.MI":"Xtrackers Digital Infrastructure","XDG7.MI":"Xtrackers Digital Payments",
        "XDGI.MI":"Xtrackers Digital Globalisation","FAMMWF.MI":"Fineco AM MSCI World Financials UCITS ETF"}},
    "media":{"name":"🎮 MEDIA & ENTERTAINMENT","color":"#0550ae","tickers":{
        "ESPO.MI":"VanEck Video Gaming & Esports ETF","ESPY.MI":"L&G Cyber Security Innovation UCITS ETF",
        "ESGO.MI":"Global X Video Games & Esports ETF","MCHT.MI":"Invesco MSCI China Technology UCITS ETF",
        "MTVS.MI":"WisdomTree Metaverse ETF","MTAV.MI":"iShares Metaverse UCITS ETF",
        "GLUX.MI":"Amundi S&P Global Luxury ETF","BRIJ.MI":"Global X European Infrastructure Dev UCITS ETF",
        "HNSC.MI":"iShares Hang Seng Tech ETF","XS8R.MI":"Xtrackers S&P 500 Info Tech",
        "XG11.MI":"Xtrackers MSCI World Growth ETF","XG12.MI":"Xtrackers MSCI World Value ETF"}},
    "acqua":{"name":"🌊 ACQUA & AMBIENTE","color":"#0969da","tickers":{
        "AQWA.MI":"iShares Global Water ETF","WATC.MI":"Amundi MSCI Water UCITS ETF",
        "WTRE.MI":"WisdomTree Water ETF","GLUG.MI":"iShares Global Water UCITS ETF",
        "HYDE.MI":"VanEck Circular Economy ETF","SDG9.MI":"iShares MSCI Global Sustainable",
        "WGRO.MI":"WisdomTree Global Growth ETF","ROBO.MI":"Robo Global Robotics ETF",
        "BOTZ.MI":"Global X Robotics & AI ETF"}},
    "semiconduttori":{"name":"💾 SEMICONDUTTORI","color":"#6e40c9","tickers":{
        "SMH.MI":"VanEck Semiconductor ETF","SEME.MI":"iShares MSCI Global Semiconductors",
        "CHIP.MI":"Amundi PHLX Semiconductor ETF","FCHP.DE":"Franklin FTSE Semiconductor ETF",
        "USTEC.MI":"iShares NASDAQ 100 UCITS ETF","FAMMWS.MI":"Fineco AM MSCI World Semiconductors UCITS ETF"}},
    "altro":{"name":"🏥 ALTRO TEMATICO","color":"#57606a","tickers":{
        "HERU.MI":"HANetf European Equity ETF","DMAT.MI":"iShares Diversified Materials ETF",
        "ROE.MI":"iShares Return on Equity ETF","QNTM.MI":"VanEck Quantum Computing UCITS ETF",
        "QUAD.MI":"Invesco Quantitative ETF","RAYZ.MI":"Rize Sustainable Future Food ETF",
        "MILL.MI":"Global X Millennials ETF","MLPS.MI":"Global X MLP ETF",
        "WMGT.MI":"WisdomTree Quality Dividend ETF","WBLK.MI":"WisdomTree Blockchain ETF",
        "ISAG.MI":"iShares Ageing Population UCITS ETF EUR","KWBE.MI":"L&G ROBO Global Robotics ETF",
        "IVAI.MI":"Invesco Artificial Intelligence UCITS ETF","IVDF.DE":"Invesco Defence Innovation ETF",
        "CAUT.MI":"iShares Automation & Robotics ETF","FAMWCS.MI":"Fineco AM MSCI World Consumer Staples UCITS ETF"}},
}
XEON_TICKER = "XEON.MI"

# ── INDICATORI ────────────────────────────────────────────────────────────
def ema_arr(v,p):
    k=2/(p+1);r=[v[0]]
    for x in v[1:]:r.append(x*k+r[-1]*(1-k))
    return r

def calc_kama(prices,n=KAMA_N,fast=KAMA_FAST,slow=KAMA_SLOW):
    fsc=2/(fast+1);ssc=2/(slow+1);result=list(prices[:n])
    for i in range(n,len(prices)):
        d=abs(prices[i]-prices[i-n])
        v=sum(abs(prices[j]-prices[j-1]) for j in range(i-n+1,i+1))
        er=d/v if v>0 else 0;sc=(er*(fsc-ssc)+ssc)**2
        result.append(result[-1]+sc*(prices[i]-result[-1]))
    return result

def calc_er(prices,n=10):
    if len(prices)<n+1:return 0.0
    d=abs(prices[-1]-prices[-n-1])
    v=sum(abs(prices[i]-prices[i-1]) for i in range(-n,0))
    return round(d/v if v>0 else 0,4)

def calc_ao_baffetti(high,low):
    mid=[(h+l)/2 for h,l in zip(high,low)]
    if len(mid)<34:return 0.0,0
    series=[sum(mid[i-4:i+1])/5-sum(mid[i-33:i+1])/34 for i in range(33,len(mid))]
    baff=0
    for i in range(len(series)-1,0,-1):
        if series[i]>series[i-1]:baff+=1
        else:break
    return round(series[-1],6),baff

def calc_ao_series(high,low,n=20):
    mid=[(h+l)/2 for h,l in zip(high,low)]
    if len(mid)<34:return []
    series=[sum(mid[i-4:i+1])/5-sum(mid[i-33:i+1])/34 for i in range(33,len(mid))]
    return series[-n:]

def calc_rsi(prices,n=14):
    if len(prices)<n+1:return 50.0
    d=[prices[i]-prices[i-1] for i in range(1,len(prices))]
    g=[max(x,0) for x in d];l=[max(-x,0) for x in d]
    ag,al=sum(g[:n])/n,sum(l[:n])/n
    for i in range(n,len(g)):ag=(ag*(n-1)+g[i])/n;al=(al*(n-1)+l[i])/n
    return round(100-100/(1+ag/al) if al>0 else 100,1)

def calc_atr(high,low,close,n=14):
    if len(close)<2:return 0.0
    tr=[max(high[i]-low[i],abs(high[i]-close[i-1]),abs(low[i]-close[i-1])) for i in range(1,len(close))]
    av=sum(tr[:n])/min(n,len(tr))
    for i in range(min(n,len(tr)),len(tr)):av=(av*(n-1)+tr[i])/n
    return round(av,5)

def calc_sar(high,low,af0=0.02,af_max=0.2):
    if len(high)<5:return low[-1],True,[True]
    sar,ep,af,bull=low[0],high[0],af0,True;sars=[sar];bulls=[True]
    for i in range(1,len(high)):
        prev=sars[-1]
        if bull:
            new=prev+af*(ep-prev)
            c=low[max(0,i-2):i];new=min(new,min(c)) if c else new
            if low[i]<new:bull,new,ep,af=False,ep,low[i],af0
            else:
                if high[i]>ep:ep=high[i];af=min(af+af0,af_max)
        else:
            new=prev+af*(ep-prev)
            c=high[max(0,i-2):i];new=max(new,max(c)) if c else new
            if high[i]>new:bull,new,ep,af=True,ep,high[i],af0
            else:
                if low[i]<ep:ep=low[i];af=min(af+af0,af_max)
        sars.append(new);bulls.append(bull)
    return round(sars[-1],5),bull,bulls[-10:]

def trendycator(close):
    if len(close)<55:return "GRIGIO"
    e21=ema_arr(close,21);e55=ema_arr(close,55)
    if close[-1]>e21[-1]>e55[-1]:return "VERDE"
    if close[-1]<e21[-1]<e55[-1]:return "ROSSO"
    return "GRIGIO"

def calc_vortex(high,low,close,n=14):
    if len(close)<n+1:return 1.0,1.0,False
    vm_p=[abs(high[i]-low[i-1]) for i in range(1,len(close))]
    vm_m=[abs(low[i]-high[i-1]) for i in range(1,len(close))]
    tr=[max(high[i]-low[i],abs(high[i]-close[i-1]),abs(low[i]-close[i-1])) for i in range(1,len(close))]
    ts=sum(tr[-n:]);vip=round(sum(vm_p[-n:])/ts if ts>0 else 1,4);vim=round(sum(vm_m[-n:])/ts if ts>0 else 1,4)
    return vip,vim,vip>vim

def calc_rvi(close,open_,high,low,n=10):
    if len(close)<n+4:return 0.0,0.0,False
    num,den=[],[]
    for i in range(3,len(close)):
        nv=(close[i]-open_[i]+2*(close[i-1]-open_[i-1])+2*(close[i-2]-open_[i-2])+(close[i-3]-open_[i-3]))/6
        dv=(high[i]-low[i]+2*(high[i-1]-low[i-1])+2*(high[i-2]-low[i-2])+(high[i-3]-low[i-3]))/6
        num.append(nv);den.append(dv)
    if len(num)<n:return 0.0,0.0,False
    rs=[sum(num[i-n+1:i+1])/(sum(den[i-n+1:i+1]) or 1) for i in range(n-1,len(num))]
    if len(rs)<4:return 0.0,0.0,False
    ss=[(rs[i]+2*rs[i-1]+2*rs[i-2]+rs[i-3])/6 for i in range(3,len(rs))]
    if not ss:return 0.0,0.0,False
    return round(rs[-1],6),round(ss[-1],6),rs[-1]>ss[-1]

def calc_score(er,baff,k_pct,p7,p30,ao_pos,cross,trend,vortex_bull=False,rvi_bull=False):
    s=(er*18+min(baff,5)*2+min(abs(k_pct),5)*1+max(-5,min(5,p7))*2
       +max(-10,min(10,p30))*0.5+(3 if ao_pos else 0)
       +(8 if cross<=3 else 5 if cross<=10 else 2 if cross<=20 else 0))
    if vortex_bull:s+=2
    if rvi_bull:s+=2
    if not vortex_bull and not rvi_bull:s-=4
    if trend=="ROSSO":s*=0.6
    return round(max(0,min(75,s)),1)

# ── BUY LEVEL ─────────────────────────────────────────────────────────────
def eval_buy_level(cur,vix_regime="NORMALE"):
    score=cur["score"];er=cur["er"];price=cur["price"];kama_v=cur["kama"]
    sar_bull=cur["sar_bullish"];vortex_b=cur.get("vortex_bullish",False)
    rvi_b=cur.get("rvi_bullish",False);baff=cur.get("baffetti",0)
    ao_val=cur["ao"];ao_series=cur.get("ao_series",[])
    trend=cur["trend"];cross_bars=cur.get("cross_bars",99)
    above_kama=price>kama_v
    if not above_kama:return None
    # BUY3
    if (score>=60 and er>0.6 and baff>=5 and vortex_b and rvi_b
            and sar_bull and trend=="VERDE" and vix_regime in ("NORMALE","RIDOTTO")):
        return "BUY3"
    # BUY2
    if (score>=45 and ao_val>0 and er>=0.5 and sar_bull and above_kama
            and vix_regime in ("NORMALE","RIDOTTO")):
        return "BUY2"
    # BUY1
    if (vix_regime=="NORMALE" and cross_bars<=5 and er>0.35 and sar_bull):
        ao_improving=len(ao_series)>=3 and ao_series[-1]>ao_series[-2]
        if ao_improving:return "BUY1"
    return None

def buy_reason(level,cur):
    if level=="BUY3":return f"Score {cur['score']}≥60 · ER {cur['er']:.2f}>0.6 · Baff {cur.get('baffetti',0)}≥5 · Vortex+RVI bull · Trend VERDE"
    if level=="BUY2":return f"Score {cur['score']}≥45 · AO>0 · ER {cur['er']:.2f}≥0.5 · SAR bull"
    if level=="BUY1":return f"KAMA cross≤5 barre · ER {cur['er']:.2f}>0.35 · SAR bull · AO migliorante"
    return level

# ── EXIT / DOWNGRADE ──────────────────────────────────────────────────────
def eval_exit(pos,cur):
    score=cur["score"];peak_score=pos.get("peak_score",score)
    if peak_score==score:peak_score=score+8
    er=cur["er"];ao_series=cur.get("ao_series",[]);sar_history=cur.get("sar_history",[])
    k_pct=cur["k_pct"];current_level=pos.get("current_level","BUY1")
    days_in_buy1=pos.get("days_in_buy1",0);trend=cur["trend"]
    vortex_b=cur.get("vortex_bullish",False);rvi_b=cur.get("rvi_bullish",False)
    baff=cur.get("baffetti",0)
    # EXIT3
    exit3_thr={"BUY3":28,"BUY2":22,"BUY1":18,"EXIT1":18,"EXIT2":18}.get(current_level,22)
    if score<exit3_thr:return "EXIT3",f"Score {score}<{exit3_thr} soglia {current_level}"
    if k_pct<0 and trend!="VERDE":return "EXIT3",f"KAMA rotta (K%={k_pct:.2f}) · Trend {trend}"
    # EXIT2
    drop=peak_score-score
    if drop>=8 and er<0.4:return "EXIT2",f"Score -{drop:.0f}pt ({peak_score:.0f}→{score:.0f}) · ER {er:.2f}<0.4"
    # EXIT1b
    if current_level=="BUY1" and days_in_buy1>=8:return "EXIT1b",f"Time stop: {days_in_buy1}gg in BUY1"
    # DOWNGRADE BUY3→BUY2
    if current_level=="BUY3":
        lost=(er<0.6 or baff<5 or not(vortex_b and rvi_b))
        if lost:
            r=[]
            if er<0.6:r.append(f"ER {er:.2f}<0.6")
            if baff<5:r.append(f"Baff {baff}<5")
            if not(vortex_b and rvi_b):r.append("Vortex/RVI indeboliti")
            return "DOWNGRADE_BUY2","BUY3→BUY2: "+" · ".join(r)
    # DOWNGRADE BUY2→BUY1
    if current_level=="BUY2":
        lost=(score<45 or er<0.5)
        if lost:
            r=[]
            if score<45:r.append(f"Score {score}<45")
            if er<0.5:r.append(f"ER {er:.2f}<0.5")
            return "DOWNGRADE_BUY1","BUY2→BUY1: "+" · ".join(r)
    # EXIT1
    sar_flipped=(len(sar_history)>=2 and not sar_history[-1] and sar_history[-2])
    ao_weakening=(len(ao_series)>=4 and ao_series[-1]<ao_series[-2]<ao_series[-3]<ao_series[-4])
    if sar_flipped:return "EXIT1","SAR girato sopra prezzo"
    if ao_weakening:return "EXIT1","AO in calo 3+ sessioni"
    return None,None

# ── STRESS ────────────────────────────────────────────────────────────────
def calc_stress(pos,cur):
    flags=[];score=cur["score"];peak_score=pos.get("peak_score",score);drop=peak_score-score
    if cur["k_pct"]<0:flags.append("KAMA rotta")
    if not cur["sar_bullish"]:flags.append("SAR bear")
    ao_s=cur.get("ao_series",[])
    if len(ao_s)>=3 and ao_s[-1]<ao_s[-2]<ao_s[-3]:flags.append("AO calante")
    elif cur["ao"]<0:flags.append("AO negativo")
    if cur["er"]<0.4:flags.append("ER<0.4")
    if drop>=6:flags.append(f"Score -{drop:.0f}pt dal picco")
    return flags,len(flags)

def now_ts():return datetime.now(ROME_TZ).strftime("%Y-%m-%d %H:%M CET")
def make_event(level,reason,size_pct,score,er):
    return {"ts":now_ts(),"level":level,"reason":reason,"size_pct":size_pct,"score":score,"er":round(er,2)}

# ── ANALYZE ───────────────────────────────────────────────────────────────
def analyze(ticker,name):
    try:
        df=yf.download(ticker,period="1y",interval="1d",progress=False,auto_adjust=True)
        if df is None or df.empty:return None
        if isinstance(df.columns,pd.MultiIndex):df.columns=df.columns.get_level_values(0)
        df=df.loc[:,~df.columns.duplicated()].dropna(subset=['Close','High','Low'])
        if len(df)<40:return None
        close=[float(x) for x in df['Close'].tolist()]
        high=[float(x) for x in df['High'].tolist()]
        low=[float(x) for x in df['Low'].tolist()]
        open_=[float(x) for x in df['Open'].tolist()] if 'Open' in df.columns else close[:]
        price=close[-1];kama_s=calc_kama(close);kama_v=kama_s[-1]
        k_pct=round((price/kama_v-1)*100 if kama_v else 0,2)
        er=calc_er(close);ao,baff=calc_ao_baffetti(high,low)
        ao_series=calc_ao_series(high,low)
        rsi_v=calc_rsi(close);atr_v=calc_atr(high,low,close)
        trail=round(price-TRAIL_MULT*atr_v,3);trend=trendycator(close)
        sar_v,sar_bull,sar_hist=calc_sar(high,low)
        vi_plus,vi_minus,vortex_bull=calc_vortex(high,low,close)
        rvi_val,rvi_sig,rvi_bull=calc_rvi(close,open_,high,low)
        p7=round((price/close[-8]-1)*100 if len(close)>=8 else 0,2)
        p30=round((price/close[-31]-1)*100 if len(close)>=31 else 0,2)
        cross_bars=0;above=price>kama_v
        for i in range(len(kama_s)-1,0,-1):
            if (close[i]>kama_s[i])==above:cross_bars+=1
            else:break
        score=calc_score(er,baff,k_pct,p7,p30,ao>0,cross_bars,trend,vortex_bull,rvi_bull)
        cur_data={"ticker":ticker,"name":name,"price":round(price,3),"kama":round(kama_v,3),
                  "k_pct":k_pct,"er":er,"ao":ao,"ao_series":ao_series,"baffetti":baff,
                  "rsi":rsi_v,"atr":atr_v,"trailing_stop":trail,"trend":trend,
                  "sar":sar_v,"sar_bullish":sar_bull,"sar_history":sar_hist,
                  "vi_plus":vi_plus,"vi_minus":vi_minus,"vortex_bullish":vortex_bull,
                  "rvi":rvi_val,"rvi_signal":rvi_sig,"rvi_bullish":rvi_bull,
                  "perf7":p7,"perf30":p30,"score":score,"cross_bars":cross_bars}
        buy_level=eval_buy_level(cur_data)
        cur_data["buy_level"]=buy_level
        cur_data["level_ts"]=now_ts() if buy_level else None
        cur_data["qualifies"]=buy_level is not None
        # TradingView rating
        tv = get_tv_rating(ticker)
        if tv: cur_data.update(tv)
        else:  cur_data.update({"tv_rating":None,"tv_buy":None,"tv_sell":None,"tv_neutral":None})
        return cur_data
    except Exception as e:
        print(f"  x {ticker}: {e}");return None

# ── STATE ─────────────────────────────────────────────────────────────────
STATE_FILE="portfolio_state.json"
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:return json.load(f)
    return {k:{"positions":[],"history":[],"cooldowns":{}} for k in GROUPS}
def save_state(state):
    with open(STATE_FILE,"w") as f:json.dump(state,f,indent=2,default=str)

# ── UPDATE PORTFOLIO ──────────────────────────────────────────────────────
def update_portfolio(gk,existing,cooldowns_raw,candidates,today_str):
    today=date.fromisoformat(today_str);kept=[];exited=[]
    cooldowns={k:v for k,v in cooldowns_raw.items() if date.fromisoformat(v)>today}
    for pos in existing:
        cur=next((c for c in candidates if c["ticker"]==pos["ticker"]),None)
        if cur is None:pos["warning"]="Dati non disponibili";kept.append(pos);continue
        entry_date=date.fromisoformat(pos["entry_date"]);days_held=(today-entry_date).days
        cur_gain=round((cur["price"]/pos["entry_price"]-1)*100,2)
        new_trail=round(cur["price"]-TRAIL_MULT*cur["atr"],3)
        trail_stop=max(pos.get("trailing_stop",new_trail),new_trail)
        target_price=pos.get("target_price",round(pos["entry_price"]*(1+TARGET_PCT/100),3))
        target_hit=cur_gain>=TARGET_PCT
        current_level=pos.get("current_level","BUY1")
        peak_score=max(pos.get("peak_score",cur["score"]),cur["score"])
        days_in_buy1=pos.get("days_in_buy1",0)+(1 if current_level=="BUY1" else 0)
        events=pos.get("events",[])
        if not events:
            ev0=make_event(current_level,f"Ingresso storico — score {cur['score']} · ER {cur['er']:.2f}",
                           SIZE_MAP.get(current_level,35),cur["score"],cur["er"])
            ev0["ts"]=pos.get("entry_ts",pos.get("entry_date",today_str)+" 09:00 CET")
            events=[ev0]
        stress_flags,stress_score=calc_stress({**pos,"peak_score":peak_score},cur)
        exit_level,exit_reason=eval_exit(
            {**pos,"peak_score":peak_score,"current_level":current_level,"days_in_buy1":days_in_buy1},cur)
        if exit_level in ("EXIT3","EXIT1b"):
            ev=make_event(exit_level,exit_reason,100,cur["score"],cur["er"])
            exited.append({**pos,"events":events+[ev],"exit_level":exit_level,
                           "exit_reason":exit_reason,"exit_price":cur["price"],
                           "final_gain_pct":cur_gain,"exit_date":today_str,"exit_ts":now_ts()})
            cooldowns[pos["ticker"]]=(today+timedelta(days=COOLDOWN_DAYS)).isoformat();continue
        if exit_level=="DOWNGRADE_BUY2":
            if not events or events[-1]["level"]!="DOWNGRADE_BUY2":
                events.append(make_event("DOWNGRADE_BUY2",exit_reason,SIZE_MAP["EXIT1"],cur["score"],cur["er"]))
            current_level="BUY2";days_in_buy1=0
        elif exit_level=="DOWNGRADE_BUY1":
            if not events or events[-1]["level"]!="DOWNGRADE_BUY1":
                events.append(make_event("DOWNGRADE_BUY1",exit_reason,SIZE_MAP["EXIT2"],cur["score"],cur["er"]))
            current_level="BUY1";days_in_buy1=1
        elif exit_level in ("EXIT1","EXIT2"):
            if not events or events[-1]["level"]!=exit_level:
                events.append(make_event(exit_level,exit_reason,SIZE_MAP[exit_level],cur["score"],cur["er"]))
            current_level=exit_level
        else:
            new_level=eval_buy_level(cur)
            if new_level and LEVEL_ORDER.get(new_level,0)>LEVEL_ORDER.get(current_level,0):
                events.append(make_event(new_level,buy_reason(new_level,cur),SIZE_MAP[new_level],cur["score"],cur["er"]))
                current_level=new_level
                if new_level!="BUY1":days_in_buy1=0
        last_ev=events[-1] if events else None
        level_ts=last_ev["ts"] if last_ev else pos.get("entry_ts",today_str)
        pos.update({"current_price":cur["price"],"current_gain_pct":cur_gain,"days_held":days_held,
                    "days_in_buy1":days_in_buy1,"trailing_stop":trail_stop,"target_price":target_price,
                    "target_hit":target_hit,"current_level":current_level,"buy_level":current_level,
                    "level_ts":level_ts,"peak_score":peak_score,"score":cur["score"],"er":cur["er"],
                    "trend":cur["trend"],"sar_bullish":cur["sar_bullish"],
                    "vortex_bullish":cur.get("vortex_bullish",False),"rvi_bullish":cur.get("rvi_bullish",False),
                    "perf7":cur["perf7"],"perf30":cur["perf30"],
                    "stress_flags":stress_flags,"stress_score":stress_score,"events":events,"warning":None,
                    "kama":cur["kama"],"k_pct":cur["k_pct"]})
        kept.append(pos)
    slots=MAX_POS-len(kept);existing_t={p["ticker"] for p in kept}|set(cooldowns.keys())
    new_q=sorted([c for c in candidates if c.get("qualifies") and c["ticker"] not in existing_t],
                 key=lambda x:x["er"],reverse=True)
    for c in new_q[:slots]:
        ep=c["price"];stop=round(max(ep-TRAIL_MULT*c["atr"],c["trailing_stop"]),3)
        level=eval_buy_level(c) or "BUY1";entry_ts=now_ts()
        ev0=make_event(level,buy_reason(level,c),SIZE_MAP.get(level,35),c["score"],c["er"])
        ev0["ts"]=entry_ts
        kept.append({"ticker":c["ticker"],"name":c["name"],"entry_date":today_str,"entry_ts":entry_ts,
                     "entry_price":ep,"current_price":ep,"target_price":round(ep*(1+TARGET_PCT/100),3),
                     "stop_loss":stop,"trailing_stop":stop,"current_gain_pct":0.0,"days_held":0,
                     "pre_alert":False,"target_hit":False,"score":c["score"],"er":c["er"],
                     "current_level":level,"buy_level":level,"level_ts":entry_ts,
                     "peak_score":c["score"],"days_in_buy1":1 if level=="BUY1" else 0,
                     "trend":c["trend"],"sar_bullish":c["sar_bullish"],
                     "vortex_bullish":c.get("vortex_bullish",False),"rvi_bullish":c.get("rvi_bullish",False),
                     "perf7":c["perf7"],"perf30":c["perf30"],
                     "stress_flags":[],"stress_score":0,"events":[ev0],"warning":None,
                     "kama":c["kama"],"k_pct":c["k_pct"]})
    total_score=sum(p["score"] for p in kept)
    for p in kept:p["weight_pct"]=round(p["score"]/total_score*100,1) if total_score else 0
    return kept,exited,cooldowns

# ── MAIN ──────────────────────────────────────────────────────────────────
def main():
    now=datetime.now(ROME_TZ);today_str=now.date().isoformat()
    print(f"🦅 RAPTOR TEMATICI v2 — {now.strftime('%d/%m/%Y %H:%M')} CET")
    state=load_state();output_groups={}
    for gk,gcfg in GROUPS.items():
        print(f"\n📊 {gcfg['name']}...")
        data=[]
        for ticker,name in gcfg["tickers"].items():
            print(f"  {ticker}...",end=" ",flush=True)
            r=analyze(ticker,name)
            if r:data.append(r);print(f"✓ s={r['score']} lvl={r.get('buy_level','—')}")
            else:print("✗")
        if gk not in state:state[gk]={"positions":[],"history":[],"cooldowns":{}}
        positions,exited,cooldowns=update_portfolio(
            gk,state[gk].get("positions",[]),state[gk].get("cooldowns",{}),data,today_str)
        state[gk]["positions"]=positions;state[gk]["cooldowns"]=cooldowns
        state[gk].setdefault("history",[]).extend(exited)
        pos_map={p["ticker"]:p for p in positions}
        for d in data:
            if d["ticker"] in pos_map:
                d["buy_level"]=pos_map[d["ticker"]].get("current_level")
                d["level_ts"]=pos_map[d["ticker"]].get("level_ts")
        output_groups[gk]={"name":gcfg["name"],"color":gcfg["color"],"positions":positions,
                           "use_xeon":len(positions)==0,"cooldowns":cooldowns,
                           "qualified":[d for d in data if d.get("qualifies")],
                           "watchlist":sorted([d for d in data if not d.get("qualifies")],
                                              key=lambda x:x["score"],reverse=True)[:6],
                           "all":sorted(data,key=lambda x:x["score"],reverse=True)}
        print(f"  → {len(positions)} pos{' | XEON' if not positions else ''}")
    print("\n💰 XEON...")
    xeon=analyze(XEON_TICKER,"Xtrackers EUR Overnight Rate Swap UCITS ETF")
    save_state(state)
    output={"updated_at":now.isoformat(),"updated_display":now.strftime("%d/%m/%Y %H:%M CET"),
            "xeon_price":xeon["price"] if xeon else None,"groups":output_groups}
    with open("tematici.json","w") as f:json.dump(output,f,indent=2,default=str)
    total=sum(len(g["positions"]) for g in output_groups.values())
    print(f"\n✅ tematici.json — {total} posizioni totali")

if __name__=="__main__":
    main()
