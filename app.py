import os
import logging
from flask import Flask, jsonify, request as flask_request, abort as flask_abort
from flask_cors import CORS
from dotenv import load_dotenv
import time
import random
import re
import hmac
import hashlib
import telebot
from telebot import types
from urllib.parse import unquote, parse_qs
from datetime import datetime as dt, timezone, timedelta
import json
from decimal import Decimal, ROUND_HALF_UP
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, DateTime, Boolean, UniqueConstraint, BigInteger
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from sqlalchemy.sql import func
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
import requests # Use the standard requests library
import math

load_dotenv()

# --- Configuration Constants ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
AUTH_DATE_MAX_AGE_SECONDS = 3600 * 24
ADMIN_USER_ID = 5146625949
WITHDRAWAL_API_URL = "https://upgrade-a57g.onrender.com/api"
WITHDRAWAL_SENDER_USERNAME = "pusikSupport"

UPGRADE_MAX_CHANCE = Decimal('75.0')
UPGRADE_MIN_CHANCE = Decimal('3.0')
UPGRADE_RISK_FACTOR = Decimal('0.60')

RTP_TARGET = Decimal('0.88')

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ludik_gifts_backend.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Basic checks for essential environment variables
if not BOT_TOKEN:
    logger.error("BOT_TOKEN not set!")
if not DATABASE_URL:
    logger.error("DATABASE_URL not set!")
    exit("DATABASE_URL is not set. Exiting.")

WEBAPP_URL = "https://vasiliy-katsyka.github.io/case"
API_BASE_URL = "https://ludik.onrender.com"
BOT_USERNAME_FOR_LINK = "upgradeDemoBot"
BIG_WIN_CHANNEL_ID = -1002786435659

# --- SQLAlchemy Database Setup ---
engine = create_engine(DATABASE_URL, pool_recycle=3600, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Database Models ---
class User(Base):
    __tablename__ = "users"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=False)
    username = Column(String, nullable=True, index=True)
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    ton_balance = Column(Float, default=0.0, nullable=False)
    referral_code = Column(String, unique=True, index=True, nullable=True)
    referred_by_id = Column(BigInteger, ForeignKey("users.id"), nullable=True)
    referral_earnings_pending = Column(Float, default=0.0, nullable=False)
    total_won_ton = Column(Float, default=0.0, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    inventory = relationship("InventoryItem", back_populates="owner", cascade="all, delete-orphan")
    referrer = relationship("User", remote_side=[id], foreign_keys=[referred_by_id], back_populates="referrals_made", uselist=False)
    referrals_made = relationship("User", back_populates="referrer")

class NFT(Base):
    __tablename__ = "nfts"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String, unique=True, index=True, nullable=False)
    image_filename = Column(String, nullable=True)
    floor_price = Column(Float, default=0.0, nullable=False)
    __table_args__ = (UniqueConstraint('name', name='uq_nft_name'),)

class InventoryItem(Base):
    __tablename__ = "inventory_items"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    nft_id = Column(Integer, ForeignKey("nfts.id"), nullable=True)
    item_name_override = Column(String, nullable=True)
    item_image_override = Column(String, nullable=True)
    current_value = Column(Float, nullable=False)
    upgrade_multiplier = Column(Float, default=1.0, nullable=False)
    obtained_at = Column(DateTime(timezone=True), server_default=func.now())
    is_ton_prize = Column(Boolean, default=False, nullable=False)
    owner = relationship("User", back_populates="inventory")
    nft = relationship("NFT")

class PromoCode(Base):
    __tablename__ = "promo_codes"
    id = Column(Integer, primary_key=True, index=True)
    code_text = Column(String, unique=True, index=True, nullable=False)
    activations_left = Column(Integer, nullable=False, default=0)
    ton_amount = Column(Float, nullable=False, default=0.0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

class UserPromoCodeRedemption(Base):
    __tablename__ = "user_promo_code_redemptions"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    promo_code_id = Column(Integer, ForeignKey("promo_codes.id", ondelete="CASCADE"), nullable=False)
    redeemed_at = Column(DateTime(timezone=True), server_default=func.now())
    user = relationship("User")
    promo_code = relationship("PromoCode")
    __table_args__ = (UniqueConstraint('user_id', 'promo_code_id', name='uq_user_promo_redemption'),)

Base.metadata.create_all(bind=engine)

bot = telebot.TeleBot(BOT_TOKEN, threaded=False) if BOT_TOKEN else None

# --- Telegram Bot Handlers ---
if bot:
    @bot.message_handler(commands=['start'])
    def send_welcome(message):
        user_id, tg_user = message.chat.id, message.from_user
        username, first_name, last_name = tg_user.username, tg_user.first_name, tg_user.last_name
        logger.info(f"User {user_id} ({username or first_name}) started the bot.")
        referral_code_found = None
        try:
            parts = message.text.split(' ')
            if len(parts) > 1 and parts[1].startswith('ref_'):
                referral_code_found = parts[1]
        except Exception as e:
            logger.error(f"Error parsing start parameter: {e}")
        if referral_code_found:
            try:
                payload = { "user_id": user_id, "username": username, "first_name": first_name, "last_name": last_name, "referral_code": referral_code_found }
                requests.post(f"{API_BASE_URL}/api/register_referral", json=payload, timeout=10)
            except Exception as e_api:
                logger.error(f"API call to register_referral failed: {e_api}")
        markup = types.InlineKeyboardMarkup()
        web_app_info = types.WebAppInfo(url=WEBAPP_URL)
        app_button = types.InlineKeyboardButton(text="🎮 Open Ludik Gifts", web_app=web_app_info)
        markup.add(app_button)
        bot.send_photo(message.chat.id, photo="https://i.ibb.co/5Q2KK6D/IMG_20250522-184911-835.jpg", caption="Welcome to Ludik Gifts! 🎁\n\nTap the button below to start!", reply_markup=markup)

# --- Gift Data & Mappings ---
TON_PRIZE_IMAGE_DEFAULT = "https://case-bot.com/images/actions/ton.svg"
GIFT_NAME_TO_ID_MAP_PY = {
  "Santa Hat": "5983471780763796287","Signet Ring": "5936085638515261992","Precious Peach": "5933671725160989227","Plush Pepe": "5936013938331222567",
  "Spiced Wine": "5913442287462908725","Jelly Bunny": "5915502858152706668","Durov's Cap": "5915521180483191380","Perfume Bottle": "5913517067138499193",
  "Eternal Rose": "5882125812596999035","Berry Box": "5882252952218894938","Vintage Cigar": "5857140566201991735","Magic Potion": "5846226946928673709",
  "Kissed Frog": "5845776576658015084","Hex Pot": "5825801628657124140","Evil Eye": "5825480571261813595","Sharp Tongue": "5841689550203650524",
  "Trapped Heart": "5841391256135008713","Skull Flower": "5839038009193792264","Scared Cat": "5837059369300132790","Spy Agaric": "5821261908354794038",
  "Homemade Cake": "5783075783622787539","Genie Lamp": "5933531623327795414","Lunar Snake": "6028426950047957932","Party Sparkler": "6003643167683903930",
  "Jester Hat": "5933590374185435592","Witch Hat": "5821384757304362229","Hanging Star": "5915733223018594841","Love Candle": "5915550639663874519",
  "Cookie Heart": "6001538689543439169","Desk Calendar": "5782988952268964995","Jingle Bells": "6001473264306619020","Snow Mittens": "5980789805615678057",
  "Voodoo Doll": "5836780359634649414","Mad Pumpkin": "5841632504448025405","Hypno Lollipop": "5825895989088617224","B-Day Candle": "5782984811920491178",
  "Bunny Muffin": "5935936766358847989","Astral Shard": "5933629604416717361","Flying Broom": "5837063436634161765","Crystal Ball": "5841336413697606412",
  "Eternal Candle": "5821205665758053411","Swiss Watch": "5936043693864651359","Ginger Cookie": "5983484377902875708","Mini Oscar": "5879737836550226478",
  "Lol Pop": "5170594532177215681","Ion Gem": "5843762284240831056","Star Notepad": "5936017773737018241","Loot Bag": "5868659926187901653",
  "Love Potion": "5868348541058942091","Toy Bear": "5868220813026526561","Diamond Ring": "5868503709637411929","Sakura Flower": "5167939598143193218",
  "Sleigh Bell": "5981026247860290310","Top Hat": "5897593557492957738","Record Player": "5856973938650776169","Winter Wreath": "5983259145522906006",
  "Snow Globe": "5981132629905245483","Electric Skull": "5846192273657692751","Tama Gadget": "6023752243218481939","Candy Cane": "6003373314888696650",
  "Neko Helmet": "5933793770951673155","Jack-in-the-Box": "6005659564635063386","Easter Egg": "5773668482394620318",
  "Bonded Ring": "5870661333703197240", "Pet Snake": "6023917088358269866", "Snake Box": "6023679164349940429",
  "Xmas Stocking": "6003767644426076664", "Big Year": "6028283532500009446", "Gem Signet": "5859442703032386168",
  "Light Sword": "5897581235231785485", "Restless Jar": "5870784783948186838", "Nail Bracelet": "5870720080265871962",
  "Heroic Helmet": "5895328365971244193", "Bow Tie": "5895544372761461960", "Heart Locket": "5868455043362980631",
  "Lush Bouquet": "5871002671934079382", "Whip Cupcake": "5933543975653737112", "Joyful Bundle": "5870862540036113469",
  "Cupid Charm": "5868561433997870501", "Valentine Box": "5868595669182186720", "Snoop Dogg": "6014591077976114307",
  "Swag Bag": "6012607142387778152", "Snoop Cigar": "6012435906336654262", "Low Rider": "6014675319464657779",
  "Westside Sign": "6014697240977737490"
}
def generate_image_filename_from_name(name_str: str) -> str:
    if not name_str: return 'placeholder.png'
    if name_str == "Dildo": return "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_003931098.png"
    if name_str == "Skebob": return "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/refs/heads/main/BackgroundEraser_20250718_034626591.png"
    if name_str == "Baggin' Cat": return "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/refs/heads/main/IMG_20250718_234950_164.png"
    if name_str == "placeholder_nothing.png": return 'https://images.emojiterra.com/mozilla/512px/274c.png'
    if "TON" in name_str.upper() and "PRIZE" in name_str.upper(): return TON_PRIZE_IMAGE_DEFAULT
    gift_id = GIFT_NAME_TO_ID_MAP_PY.get(name_str)
    if gift_id: return f"https://cdn.changes.tg/gifts/originals/{gift_id}/Original.png"
    cleaned = re.sub(r'\s+', '-', name_str.replace('&', 'and').replace("'", ""))
    filename = re.sub(r'-+', '-', cleaned)
    if not filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg')): filename += '.png'
    return filename

UPDATED_FLOOR_PRICES = {
    'Plush Pepe': 3024.0, 'Neko Helmet': 15.0, 'Sharp Tongue': 17.0, "Durov's Cap": 420.0, 'Voodoo Doll': 9.4, 'Vintage Cigar': 24.0, 'Astral Shard': 80.0, 'Scared Cat': 22.0, 'Swiss Watch': 25.0, 'Perfume Bottle': 88.0, 'Precious Peach': 270.0, 'Toy Bear': 16.3, 'Genie Lamp': 46.0, 'Loot Bag': 45.0, 'Kissed Frog': 24.0, 'Electric Skull': 10.9, 'Diamond Ring': 12.0, 'Mini Oscar': 40.5, 'Party Sparkler': 2.0, 'Homemade Cake': 2.0, 'Cookie Heart': 1.8, 'Jack-in-the-box': 2.0, 'Skull Flower': 3.4, 'Lol Pop': 1.1, 'Hypno Lollipop': 1.4, 'Desk Calendar': 1.1, 'B-Day Candle': 1.4, 'Record Player': 4.0, 'Jelly Bunny': 3.6, 'Tama Gadget': 4.0, 'Snow Globe': 2.0, 'Eternal Rose': 11.0, 'Love Potion': 5.4, 'Top Hat': 6.0, 'Berry Box': 4.1, 'Bunny Muffin': 4.0, 'Candy Cane': 1.6, 'Crystal Ball': 6.0, 'Easter Egg': 1.8, 'Eternal Candle': 3.1, 'Evil Eye': 4.2, 'Flying Broom': 4.5, 'Ginger Cookie': 2.7, 'Hanging Star': 4.1, 'Hex Pot': 3.1, 'Ion Gem': 44.0, 'Jester Hat': 2.0, 'Jingle Bells': 1.8, 'Love Candle': 6.7, 'Lunar Snake': 1.5, 'Mad Pumpkin': 6.2, 'Magic Potion': 54.0, 'Pet Snake': 3.2, 'Sakura Flower': 4.1, 'Santa Hat': 2.0, 'Signet Ring': 18.8, 'Sleigh Bell': 6.0, 'Snow Mittens': 2.9, 'Spiced Wine': 2.2, 'Spy Agaric': 2.8, 'Star Notepad': 2.8, 'Trapped Heart': 6.0, 'Winter Wreath': 2.0, "Big Year": 4.4, "Snake Box": 3.3, "Bonded Ring": 60.5, "Xmas Stocking": 2.5, "Dildo": 43.0, "Skebob": 10.0, "Baggin' Cat": 3.0, "Restless Jar": 4.0, "Nail Bracelet": 70.0, "Heroic Helmet": 190.0, "Bow Tie": 3.0, "Heart Locket": 990.0, "Lush Bouquet": 3.0, "Whip Cupcake": 2.0, "Joyful Bundle": 3.0, "Cupid Charm": 12.0, "Valentine Box": 4.0, "Snoop Dogg": 2.12, "Swag Bag": 2.12, "Snoop Cigar": 4.0, "Low Rider": 30.0, "Westside Sign": 98.0
}

# --- RTP Calculation Functions ---
def calculate_rtp_probabilities(case_data, all_floor_prices):
    case_price = Decimal(str(case_data['priceTON']))
    target_ev = case_price * RTP_TARGET
    prizes = []
    for p_info in case_data['prizes']:
        prize_name = p_info['name']
        floor_price = Decimal(str(all_floor_prices.get(prize_name, 0)))
        image_filename = p_info.get('imageFilename', generate_image_filename_from_name(prize_name))
        prizes.append({'name': prize_name, 'probability': Decimal(str(p_info['probability'])), 'floor_price': floor_price, 'imageFilename': image_filename, 'is_ton_prize': p_info.get('is_ton_prize', False)})
    if not prizes or all(p['floor_price'] == 0 for p in prizes): return []
    filler_prize = min((p for p in prizes if p['floor_price'] > 0), key=lambda p: p['floor_price'], default=None)
    if not filler_prize: return calculate_rtp_probabilities_proportional_fallback(case_data, all_floor_prices)
    sum_non_filler_ev = sum(p['floor_price'] * p['probability'] for p in prizes if p is not filler_prize)
    non_filler_prob = sum(p['probability'] for p in prizes if p is not filler_prize)
    rem_ev = target_ev - sum_non_filler_ev
    if filler_prize['floor_price'] <= 0: return calculate_rtp_probabilities_proportional_fallback(case_data, all_floor_prices)
    req_filler_prob = rem_ev / filler_prize['floor_price']
    if not (0 <= req_filler_prob <= 1): return calculate_rtp_probabilities_proportional_fallback(case_data, all_floor_prices)
    if non_filler_prob > 0:
        scale = (1 - req_filler_prob) / non_filler_prob
        for p in prizes:
            if p is not filler_prize: p['probability'] *= scale
    filler_prize['probability'] = req_filler_prob
    sum_probs = sum(p['probability'] for p in prizes)
    if abs(sum_probs - 1) > 1e-7: prizes[0]['probability'] += 1 - sum_probs
    return [{'name': p['name'], 'probability': float(p['probability']), 'floor_price': float(p['floor_price']), 'imageFilename': p['imageFilename'], 'is_ton_prize': p['is_ton_prize']} for p in prizes]

def calculate_rtp_probabilities_proportional_fallback(case_data, all_floor_prices):
    case_price = Decimal(str(case_data['priceTON']))
    target_ev = case_price * RTP_TARGET
    prizes = []
    for p_info in case_data['prizes']:
        prize_name = p_info['name']
        floor_price = Decimal(str(all_floor_prices.get(prize_name, 0)))
        prizes.append({'name': prize_name, 'probability': Decimal(str(p_info['probability'])), 'floor_price': floor_price, 'imageFilename': p_info.get('imageFilename', generate_image_filename_from_name(prize_name)), 'is_ton_prize': p_info.get('is_ton_prize', False)})
    current_ev = sum(p['floor_price'] * p['probability'] for p in prizes)
    if current_ev <= 0: return []
    scale = target_ev / current_ev
    for p in prizes: p['probability'] *= scale
    total_prob = sum(p['probability'] for p in prizes)
    if total_prob <= 0: return []
    for p in prizes: p['probability'] /= total_prob
    sum_probs = sum(p['probability'] for p in prizes)
    if abs(sum_probs - 1) > 1e-7: prizes[0]['probability'] += 1 - sum_probs
    return [{'name': p['name'], 'probability': float(p['probability']), 'floor_price': float(p['floor_price']), 'imageFilename': p['imageFilename'], 'is_ton_prize': p['is_ton_prize']} for p in prizes]

# --- Game Data ---
cases_data_backend_raw = [
    {'id':'all_in_01','name':'All In', 'imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/All-In.jpg', 'priceTON':0.1,'prizes': sorted([
        {'name':'Heart Locket','probability': 0.0000001}, {'name':'Plush Pepe','probability': 0.0000005}, {'name':'Durov\'s Cap','probability': 0.000005}, {'name':'Heroic Helmet','probability': 0.00001}, {'name':'Precious Peach','probability': 0.00002}, {'name':'Bonded Ring','probability': 0.00005}, {'name':'Lol Pop','probability': 0.001}, {'name':'Baggin\' Cat','probability': 0.01}, {'name':'Whip Cupcake','probability': 0.02}, {'name':'Nothing','probability': 0.9689144, 'imageFilename': 'placeholder_nothing.png'}
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},
    {'id':'small_billionaire_05','name':'Small Billionaire', 'imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Small-Billionaire.jpg', 'priceTON':0.5,'prizes': sorted([
        {'name':'Westside Sign','probability': 0.00005}, {'name':'Perfume Bottle','probability': 0.00015}, {'name':'Nail Bracelet','probability': 0.00016}, {'name':'Vintage Cigar','probability': 0.00018}, {'name':'Signet Ring','probability': 0.0002}, {'name':'Swiss Watch','probability': 0.00022}, {'name':'Low Rider','probability': 0.0005}, {'name':'Skebob','probability': 0.005}, {'name':'Snake Box', 'probability': 0.008}, {'name':'Snoop Dogg', 'probability': 0.02}, {'name':'Swag Bag', 'probability': 0.02}, {'name':'Nothing','probability': 0.94554, 'imageFilename': 'placeholder_nothing.png'}
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},
    {'id':'lolpop','name':'Lol Pop Stash','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Lol-Pop.jpg','priceTON':2.0,'prizes': sorted([
        {'name':'Plush Pepe','probability':0.0000001}, {'name':'Neko Helmet','probability':0.00001}, {'name':'Snake Box', 'probability': 0.0005}, {'name':'Pet Snake', 'probability': 0.0005}, {'name':'Skull Flower','probability':0.0005}, {'name':'Xmas Stocking', 'probability': 0.05}, {'name':'Spiced Wine','probability':0.05}, {'name':'Bow Tie','probability': 0.1}, {'name':'Lush Bouquet','probability': 0.1}, {'name':'Joyful Bundle','probability': 0.1}, {'name':'Baggin\' Cat','probability': 0.1}, {'name':'Party Sparkler','probability':0.1}, {'name':'Homemade Cake','probability':0.1}, {'name':'Jack-in-the-box','probability':0.1}, {'name':'Santa Hat','probability':0.0484899}
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},
    {'id':'recordplayer','name':'Record Player Vault','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Record-Player.jpg','priceTON':3.0,'prizes': sorted([
        {'name':'Plush Pepe','probability':0.0000001}, {'name':'Tama Gadget','probability':0.001}, {'name':'Record Player','probability':0.001}, {'name':'Big Year', 'probability': 0.001}, {'name':'Jelly Bunny','probability':0.001}, {'name':'Crystal Ball','probability':0.001}, {'name':'Evil Eye','probability':0.001}, {'name':'Flying Broom','probability':0.001}, {'name':'Skull Flower','probability':0.001}, {'name':'Restless Jar','probability': 0.05}, {'name':'Snoop Cigar','probability': 0.05}, {'name':'Pet Snake', 'probability': 0.05}, {'name':'Eternal Candle','probability':0.05}, {'name':'Hex Pot','probability':0.1}, {'name':'Xmas Stocking', 'probability': 0.1}, {'name':'Snow Mittens','probability':0.091}, {'name':'Spy Agaric','probability':0.1}, {'name':'Star Notepad','probability':0.1}, {'name':'Ginger Cookie','probability':0.1}, {'name':'Party Sparkler','probability':0.15}, {'name':'Lol Pop','probability':0.15}
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},
    {'id': 'girls_collection', 'name': 'Girl\'s Collection', 'imageFilename': 'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/girls.jpg', 'priceTON': 8.0, 'prizes': sorted([
        {'name': 'Loot Bag', 'probability': 0.00001}, {'name': 'Genie Lamp', 'probability': 0.00001}, {'name': 'Sharp Tongue', 'probability': 0.00001}, {'name': 'Neko Helmet', 'probability': 0.00001}, {'name': 'Toy Bear', 'probability': 0.00001}, {'name': 'Eternal Rose', 'probability': 0.0001}, {'name': 'Valentine Box','probability': 0.1}, {'name': 'Cupid Charm','probability': 0.1}, {'name': 'Berry Box', 'probability': 0.2}, {'name': 'Sakura Flower', 'probability': 0.2}, {'name': 'Bunny Muffin', 'probability': 0.19985}, {'name': 'Star Notepad', 'probability': 0.2}
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},
    {'id': 'mens_collection', 'name': 'Men\'s Collection', 'imageFilename': 'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/men.jpg', 'priceTON': 8.0, 'prizes': sorted([
        {'name': 'Durov\'s Cap', 'probability': 0.000001}, {'name': 'Mini Oscar', 'probability': 0.00001}, {'name': 'Perfume Bottle', 'probability': 0.00001}, {'name': 'Scared Cat', 'probability': 0.0001}, {'name': 'Vintage Cigar', 'probability': 0.0001}, {'name': 'Signet Ring', 'probability': 0.0001}, {'name': 'Swiss Watch', 'probability': 0.0001}, {'name': 'Dildo','probability': 0.001}, {'name': 'Top Hat', 'probability': 0.3}, {'name': 'Record Player', 'probability': 0.3}, {'name': 'Spiced Wine', 'probability': 0.399579}
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},
    {'id':'swisswatch','name':'Swiss Watch Box','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Swiss-Watch.jpg','priceTON':10.0,'prizes':sorted([
        {'name':'Plush Pepe','probability':0.0000001}, {'name':'Signet Ring','probability':0.00001}, {'name':'Swiss Watch','probability':0.00001}, {'name':'Neko Helmet','probability':0.00001}, {'name':'Eternal Rose','probability':0.00005}, {'name':'Electric Skull','probability':0.0001}, {'name':'Skebob','probability': 0.05}, {'name':'Voodoo Doll','probability':0.1}, {'name':'Diamond Ring','probability':0.1}, {'name':'Love Candle','probability':0.1}, {'name':'Mad Pumpkin','probability':0.1}, {'name':'Sleigh Bell','probability':0.05}, {'name':'Top Hat','probability':0.1}, {'name':'Trapped Heart','probability':0.0998199}, {'name':'Love Potion','probability':0.1}, {'name':'Big Year', 'probability': 0.1}, {'name':'Record Player','probability':0.1}, {'name':'Snake Box', 'probability': 0.05}
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},
    {'id':'perfumebottle','name':'Perfume Chest','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Perfume-Bottle.jpg','priceTON': 20.0,'prizes': sorted([
        {'name':'Plush Pepe','probability':0.0000001}, {'name':'Bonded Ring', 'probability': 0.0000005}, {'name':'Ion Gem','probability':0.000001}, {'name':'Perfume Bottle','probability':0.000005}, {'name':'Magic Potion','probability':0.00001}, {'name':'Loot Bag','probability':0.00001}, {'name':'Genie Lamp','probability':0.01}, {'name':'Swiss Watch','probability':0.01}, {'name':'Sharp Tongue','probability':0.02}, {'name':'Neko Helmet','probability':0.02}, {'name':'Kissed Frog','probability':0.05}, {'name':'Electric Skull','probability':0.1}, {'name':'Diamond Ring','probability':0.1}, {'name':'Big Year', 'probability': 0.1}, {'name':'Snake Box', 'probability': 0.4899734}, {'name':'Pet Snake', 'probability': 0.1}
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},
    {'id':'vintagecigar','name':'Vintage Cigar Safe','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Vintage-Cigar.jpg','priceTON':40.0,'prizes': sorted([
        {'name':'Plush Pepe','probability':0.0000001}, {'name':'Precious Peach','probability':0.000001}, {'name':'Bonded Ring', 'probability': 0.000005}, {'name':'Mini Oscar','probability':0.00001}, {'name':'Dildo','probability': 0.005}, {'name':'Perfume Bottle','probability':0.01}, {'name':'Scared Cat','probability':0.1}, {'name':'Vintage Cigar','probability':0.1}, {'name':'Swiss Watch','probability':0.05}, {'name':'Sharp Tongue','probability':0.1}, {'name':'Genie Lamp','probability':0.1}, {'name':'Toy Bear','probability':0.1349839}, {'name':'Neko Helmet','probability':0.1}, {'name':'Big Year', 'probability': 0.1}, {'name':'Snake Box', 'probability': 0.1}, {'name':'Pet Snake', 'probability': 0.1}
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},
    {'id':'astralshard','name':'Astral Shard Relic','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Astral-Shard.jpg','priceTON':100.0,'prizes': sorted([
        {'name':'Plush Pepe','probability':0.0000001}, {'name':'Durov\'s Cap','probability':0.0000005}, {'name':'Precious Peach','probability':0.000001}, {'name':'Heroic Helmet','probability': 0.000005}, {'name':'Bonded Ring', 'probability': 0.01}, {'name':'Astral Shard','probability':0.05}, {'name':'Ion Gem','probability':0.05}, {'name':'Mini Oscar','probability':0.05}, {'name':'Perfume Bottle','probability':0.05}, {'name':'Magic Potion','probability':0.05}, {'name':'Loot Bag','probability':0.0899934}, {'name':'Scared Cat','probability':0.1}, {'name':'Vintage Cigar','probability':0.1}, {'name':'Swiss Watch','probability':0.1}, {'name':'Toy Bear','probability':0.1}, {'name':'Neko Helmet','probability':0.1}, {'name':'Big Year', 'probability': 0.05}, {'name':'Pet Snake', 'probability': 0.05}
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)},
    {'id':'plushpepe','name':'Plush Pepe Hoard','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Plush-Pepe.jpg','priceTON': 200.0,'prizes': sorted([
        {'name':'Plush Pepe','probability':0.000001}, {'name':'Durov\'s Cap','probability':0.000005}, {'name':'Heart Locket','probability': 0.000001}, {'name':'Precious Peach','probability':0.4}, {'name':'Bonded Ring', 'probability': 0.3}, {'name':'Astral Shard','probability':0.299993}
    ], key=lambda p: UPDATED_FLOOR_PRICES.get(p['name'], 0), reverse=True)}
]
cases_data_backend = [ {**case, 'prizes': calculate_rtp_probabilities(case, UPDATED_FLOOR_PRICES)} for case in cases_data_backend_raw ]

# --- Initial Setup ---
def initial_setup_and_logging():
    db = SessionLocal()
    try:
        for name, price in UPDATED_FLOOR_PRICES.items():
            nft = db.query(NFT).filter(NFT.name == name).first()
            img = generate_image_filename_from_name(name)
            if not nft:
                db.add(NFT(name=name, image_filename=img, floor_price=price))
            elif nft.floor_price != price or nft.image_filename != img:
                nft.floor_price, nft.image_filename = price, img
        db.commit()
    except Exception as e:
        db.rollback(); logger.error(f"Error populating NFT data: {e}")
    finally:
        db.close()

initial_setup_and_logging()

# --- Flask App ---
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

def get_db():
    db = SessionLocal();
    try: yield db
    finally: db.close()

def validate_init_data(init_data_str: str, token: str) -> dict | None:
    if not init_data_str or not token: return None
    try:
        data = dict(parse_qs(init_data_str))
        if 'hash' not in data: return None
        hash_val = data.pop('hash')[0]
        check_str = "\n".join(sorted([f"{k}={v[0]}" for k, v in data.items()]))
        secret = hmac.new("WebAppData".encode(), token.encode(), hashlib.sha256)
        if hmac.new(secret.digest(), check_str.encode(), hashlib.sha256).hexdigest() == hash_val:
            user_data = json.loads(unquote(data['user'][0]))
            user_data['id'] = int(user_data['id'])
            return user_data
    except Exception: return None
    return None

# --- API Routes ---
@app.route('/')
def index_route(): return "Ludik Gifts API Backend is Running!"

@app.route('/api/get_user_data', methods=['POST'])
def get_user_data_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).first()
        if not user:
            user = User(id=uid, username=auth.get("username"), first_name=auth.get("first_name"), last_name=auth.get("last_name"), referral_code=f"ref_{uid}_{random.randint(1000,9999)}")
            db.add(user); db.commit(); db.refresh(user)
        inv = [{"id":i.id, "name":i.nft.name if i.nft else i.item_name_override, "imageFilename":i.item_image_override, "currentValue":i.current_value, "is_ton_prize":i.is_ton_prize} for i in user.inventory]
        refs_count = db.query(User).filter(User.referred_by_id == uid).count()
        return jsonify({"id":user.id, "username":user.username, "first_name":user.first_name, "last_name":user.last_name, "tonBalance":user.ton_balance, "inventory":inv, "referralCode":user.referral_code, "referralEarningsPending":user.referral_earnings_pending, "invited_friends_count":refs_count})
    finally:
        db.close()

@app.route('/api/instant_topup', methods=['POST'])
def instant_topup_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]
    data = flask_request.get_json()
    try:
        amount = float(data.get('amount', 0))
        if not (0.1 <= amount <= 10000): return jsonify({"error": "Invalid amount."}), 400
    except (ValueError, TypeError): return jsonify({"error": "Invalid format."}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"error": "User not found"}), 404
        user.ton_balance += amount
        db.commit()
        logger.info(f"User {uid} topped up {amount} TON. New balance: {user.ton_balance}")
        return jsonify({"status": "success", "new_balance_ton": user.ton_balance})
    finally:
        db.close()

@app.route('/api/open_case', methods=['POST'])
def open_case_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]
    data = flask_request.get_json()
    cid, mult = data.get('case_id'), int(data.get('multiplier', 1))
    if not cid or mult not in [1, 2, 3]: return jsonify({"error": "Invalid request"}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"error": "User not found"}), 404
        tcase = next((c for c in cases_data_backend if c['id'] == cid), None)
        if not tcase: return jsonify({"error": "Case not found"}), 404
        cost = Decimal(str(tcase['priceTON'])) * Decimal(mult)
        if Decimal(str(user.ton_balance)) < cost: return jsonify({"error": f"Not enough TON"}), 400
        user.ton_balance = float(Decimal(str(user.ton_balance)) - cost)
        prizes, won_prizes = tcase['prizes'], []
        for _ in range(mult):
            chosen = random.choices(prizes, weights=[p['probability'] for p in prizes], k=1)[0]
            dbnft = db.query(NFT).filter(NFT.name == chosen['name']).first()
            item = InventoryItem(user_id=uid, nft_id=dbnft.id if dbnft else None, item_name_override=chosen['name'], item_image_override=chosen.get('imageFilename'), current_value=float(chosen.get('floor_price',0)), is_ton_prize=chosen.get('is_ton_prize',False))
            db.add(item); db.flush()
            won_prizes.append({"id": item.id, "name": item.item_name_override, "imageFilename": item.item_image_override, "currentValue": item.current_value, "is_ton_prize": item.is_ton_prize})
        db.commit()
        return jsonify({"status": "success", "won_prizes": won_prizes, "new_balance_ton": user.ton_balance})
    finally:
        db.close()

@app.route('/api/withdraw_gift', methods=['POST'])
def withdraw_gift_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Authentication failed"}), 401
    uid, username = auth["id"], auth.get("username")
    if not username: return jsonify({"error": "Your Telegram account needs a username."}), 400
    data = flask_request.get_json()
    item_id = data.get('inventory_item_id')
    if not item_id: return jsonify({"error": "inventory_item_id required."}), 400
    db = next(get_db())
    try:
        item = db.query(InventoryItem).filter(InventoryItem.id == item_id, InventoryItem.user_id == uid).with_for_update().first()
        if not item: return jsonify({"error": "Item not found."}), 404
        if item.is_ton_prize: return jsonify({"error": "Cannot withdraw TON prizes."}), 400
        gift_name = item.nft.name if item.nft else item.item_name_override
        
        payload = {
            "giftname": gift_name,
            "receiverUsername": username,
            "senderUsername": WITHDRAWAL_SENDER_USERNAME
        }
        
        try:
            response = requests.post(f"{WITHDRAWAL_API_URL}/create_and_transfer_random_gift", json=payload, timeout=30)
            response.raise_for_status() # Raises an exception for 4xx or 5xx status codes
            
            # If the request was successful
            db.delete(item); db.commit()
            logger.info(f"Successfully withdrew item '{gift_name}' for user {uid} ({username}).")
            return jsonify({"status": "success", "message": f"Your '{gift_name}' has been sent!"})

        except requests.exceptions.RequestException as e:
            logger.error(f"Withdrawal API request error for user {uid}, item {gift_name}: {e}")
            error_message = "Withdrawal service is currently unavailable. Please try again later."
            if e.response is not None:
                try:
                    error_detail = e.response.json().get("error", "Unknown API error")
                    error_message = f"Withdrawal service failed: {error_detail}"
                except json.JSONDecodeError:
                    error_message = f"Withdrawal service returned an invalid response (Status {e.response.status_code})."
            
            db.rollback()
            return jsonify({"error": error_message}), 502 # Bad Gateway
            
    except Exception as e:
        db.rollback()
        logger.error(f"Error in withdraw_gift for user {uid}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred during the withdrawal process."}), 500
    finally:
        db.close()

@app.route('/api/upgrade_item_v2', methods=['POST'])
def upgrade_item_v2_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid, data = auth["id"], flask_request.get_json()
    item_id, desired_name = data.get('inventory_item_id'), data.get('desired_item_name')
    if not item_id or not desired_name: return jsonify({"error": "Missing parameters."}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        item = db.query(InventoryItem).filter(InventoryItem.id == item_id, InventoryItem.user_id == uid).with_for_update().first()
        desired_nft = db.query(NFT).filter(NFT.name == desired_name).first()
        if not user or not item or not desired_nft: return jsonify({"error": "Invalid data."}), 404
        if item.is_ton_prize or Decimal(str(item.current_value)) <= 0: return jsonify({"error": "Invalid item for upgrade."}), 400
        val_current, val_desired = Decimal(str(item.current_value)), Decimal(str(desired_nft.floor_price))
        if val_desired <= val_current: return jsonify({"error": "Desired item must be more valuable."}), 400
        x = val_desired / val_current
        chance = min(UPGRADE_MAX_CHANCE, max(UPGRADE_MIN_CHANCE, UPGRADE_MAX_CHANCE * (UPGRADE_RISK_FACTOR ** (x - 1))))
        if random.uniform(0, 100) < float(chance):
            user.total_won_ton += float(val_desired - val_current)
            db.delete(item)
            new_item = InventoryItem(user_id=uid, nft_id=desired_nft.id, item_name_override=desired_nft.name, item_image_override=desired_nft.image_filename, current_value=float(val_desired), upgrade_multiplier=1.0)
            db.add(new_item); db.commit(); db.refresh(new_item)
            return jsonify({"status": "success", "item": {"id": new_item.id, "name": new_item.item_name_override, "imageFilename": new_item.item_image_override, "currentValue": new_item.current_value}})
        else:
            user.total_won_ton -= float(val_current)
            db.delete(item); db.commit()
            return jsonify({"status": "failed", "item_lost": True})
    finally:
        db.close()

@app.route('/api/convert_to_ton', methods=['POST'])
def convert_to_ton_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]
    item_id = flask_request.json.get('inventory_item_id')
    if not item_id: return jsonify({"error": "inventory_item_id required"}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        item = db.query(InventoryItem).filter(InventoryItem.id == item_id, InventoryItem.user_id == uid).first()
        if not user or not item: return jsonify({"error": "Not found"}), 404
        if item.is_ton_prize: return jsonify({"error": "Cannot convert TON"}), 400
        val = Decimal(str(item.current_value))
        user.ton_balance = float(Decimal(str(user.ton_balance)) + val)
        user.total_won_ton = float(max(0, Decimal(str(user.total_won_ton)) - val))
        db.delete(item); db.commit()
        return jsonify({"status": "success", "new_balance_ton": user.ton_balance})
    finally:
        db.close()

@app.route('/api/sell_all_items', methods=['POST'])
def sell_all_items_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"error": "User not found"}), 404
        items_to_sell = [item for item in user.inventory if not item.is_ton_prize]
        if not items_to_sell: return jsonify({"status":"no_items","message":"No items to sell."})
        total_val = sum(Decimal(str(i.current_value)) for i in items_to_sell)
        user.ton_balance = float(Decimal(str(user.ton_balance)) + total_val)
        user.total_won_ton = float(max(0, Decimal(str(user.total_won_ton)) - total_val))
        for i_del in items_to_sell: db.delete(i_del)
        db.commit()
        return jsonify({"status":"success", "message":f"{len(items_to_sell)} items sold.", "new_balance_ton":user.ton_balance})
    finally:
        db.close()

@app.route('/api/get_leaderboard', methods=['GET'])
def get_leaderboard_api():
    db = next(get_db())
    try:
        leaders = db.query(User).order_by(User.total_won_ton.desc()).limit(100).all()
        return jsonify([{"rank": i + 1, "name": u.first_name or u.username or f"User_{str(u.id)[:4]}", "avatarChar": (u.first_name or "U")[0].upper(), "income": u.total_won_ton, "user_id": u.id} for i, u in enumerate(leaders)])
    finally:
        db.close()

@app.route('/api/withdraw_referral_earnings', methods=['POST'])
def withdraw_referral_earnings_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"error": "User not found"}), 404
        if user.referral_earnings_pending > 0:
            amount = Decimal(str(user.referral_earnings_pending))
            user.ton_balance = float(Decimal(str(user.ton_balance)) + amount)
            user.referral_earnings_pending = 0.0
            db.commit()
            return jsonify({"status":"success", "new_balance_ton":user.ton_balance, "new_referral_earnings_pending":0.0})
        return jsonify({"status":"no_earnings"})
    finally:
        db.close()

@app.route('/api/redeem_promocode', methods=['POST'])
def redeem_promocode_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]
    code = flask_request.json.get('promocode_text', "").strip()
    if not code: return jsonify({"error":"Code required."}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        promo = db.query(PromoCode).filter(PromoCode.code_text == code).with_for_update().first()
        if not user or not promo: return jsonify({"error": "Invalid data."}), 404
        if promo.activations_left != -1 and promo.activations_left <= 0: return jsonify({"error":"Code expired."}), 400
        if db.query(UserPromoCodeRedemption).filter_by(user_id=uid, promo_code_id=promo.id).first():
            return jsonify({"error": "Already redeemed."}), 400
        if promo.activations_left != -1: promo.activations_left -= 1
        user.ton_balance += float(promo.ton_amount)
        db.add(UserPromoCodeRedemption(user_id=uid, promo_code_id=promo.id)); db.commit()
        return jsonify({"status":"success", "message":f"Redeemed! +{promo.ton_amount} TON", "new_balance_ton":user.ton_balance})
    finally:
        db.close()
        
@app.route('/api/register_referral', methods=['POST'])
def register_referral_api():
    data = flask_request.get_json()
    user_id, username, first_name, last_name, ref_code = data.get('user_id'), data.get('username'), data.get('first_name'), data.get('last_name'), data.get('referral_code')
    if not user_id or not ref_code: return jsonify({"error": "Missing data"}), 400
    db = next(get_db())
    try:
        referred = db.query(User).filter(User.id == user_id).first()
        if not referred:
            referred = User(id=user_id, username=username, first_name=first_name, last_name=last_name, referral_code=f"ref_{user_id}_{random.randint(1000,9999)}")
            db.add(referred); db.flush()
        if referred.referred_by_id: return jsonify({"status": "already_referred"}), 200
        referrer = db.query(User).filter(User.referral_code == ref_code).first()
        if referrer and referrer.id != referred.id:
            referred.referred_by_id = referrer.id
            if bot:
                try: bot.send_message(referrer.id, f"🎉 New Referral! 🎉\n\nUser {referred.first_name or referred.username} joined from your link!", parse_mode="Markdown")
                except Exception as e: logger.error(f"Failed to send referral notification: {e}")
        db.commit()
        return jsonify({"status": "success"}), 200
    finally:
        db.close()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
