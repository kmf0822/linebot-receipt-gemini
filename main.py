import base64
import json
import os
import sys
import uuid
from io import BytesIO

import PIL.Image
from fastapi import FastAPI, HTTPException, Request
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    ApiClient,
    AsyncApiClient,
    AsyncMessagingApi,
    AsyncMessagingApiBlob,
    Configuration,
    FlexContainer,
    FlexMessage,
    MessagingApi,
    QuickReply,
    QuickReplyItem,
    MessageAction,
    ReplyMessageRequest,
    ShowLoadingAnimationRequest,
    TextMessage,
)
from linebot.v3.webhook import WebhookParser
from linebot.v3.webhooks import MessageEvent

from models import OpenAIModel
from src.logger import logger
from src.sheets_storage import SheetsStorage

# Environment variables
channel_secret = os.getenv("ChannelSecret")
channel_access_token = os.getenv("ChannelAccessToken")
openai_api_key = os.getenv("AZURE_OPENAI_API_KEY")
openai_model_engine = os.getenv("AZURE_OPENAI_MODEL_ENGINE")
spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")

image_prompt = """\
You are a meticulous travel secretary analyzing travel-related documents in Japan.
The document could be one of the following types:
1. Shopping receipt (收據/レシート)
2. Transportation ticket - train/shinkansen/flight (車票/乗車券/航空券)
3. Hotel/accommodation booking confirmation (住宿確認單/宿泊予約確認書)
4. Restaurant receipt (餐廳收據/飲食店レシート)
5. Attraction/event ticket (景點門票/入場券)

Return exactly one JSON object following one of the schemas below and no additional narration.

Receipt schema (for shopping/restaurant):
{
    "Receipt": [
        {
            "ReceiptID": "YYYYMMDDHHmm",
            "PurchaseStore": "",
            "PurchaseDate": "YYYY/MM/DD HH:mm",
            "PurchaseAddress": "",
            "Category": "shopping|restaurant|convenience|drugstore|souvenir|other",
            "PaymentMethod": "cash|credit|ic_card|qr_pay|other",
            "Currency": "JPY",
            "TotalAmount": "",
            "TaxAmount": "",
            "TaxFreeAmount": ""
        }
    ],
    "Items": [
        {"ItemID": "ReceiptID-01", "ReceiptID": "ReceiptID", "ItemName": "", "ItemPrice": "", "Quantity": "1"}
    ]
}

Ticket schema (for train/flight):
{
    "Ticket": [
        {
            "TicketID": "YYYYMMDDHHmmCarrier",
            "CarrierName": "",
            "RouteNumber": "",
            "TicketType": "shinkansen|jr|metro|bus|flight|ferry",
            "DepartureStation": "",
            "ArrivalStation": "",
            "DepartureTime": "YYYY/MM/DD HH:mm",
            "ArrivalTime": "YYYY/MM/DD HH:mm",
            "PassengerName": "",
            "SeatClass": "reserved|non-reserved|green|gran_class|ordinary",
            "Currency": "JPY",
            "TotalAmount": ""
        }
    ],
    "Segments": [
        {"SegmentID": "TicketID-01", "SegmentName": "", "Departure": "", "Arrival": "", "Seat": "", "CarNumber": ""}
    ]
}

Hotel schema (for accommodation):
{
    "Hotel": [
        {
            "HotelID": "YYYYMMDDHotelName",
            "HotelName": "",
            "HotelAddress": "",
            "CheckInDate": "YYYY/MM/DD",
            "CheckOutDate": "YYYY/MM/DD",
            "Nights": "",
            "RoomType": "",
            "GuestName": "",
            "Currency": "JPY",
            "TotalAmount": "",
            "ConfirmationNumber": ""
        }
    ],
    "RoomDetails": [
        {"DetailID": "HotelID-01", "Description": "", "Price": ""}
    ]
}

Attraction schema (for tickets/admissions):
{
    "Attraction": [
        {
            "AttractionID": "YYYYMMDDAttractionName",
            "AttractionName": "",
            "AttractionAddress": "",
            "VisitDate": "YYYY/MM/DD",
            "VisitTime": "HH:mm",
            "TicketType": "adult|child|student|senior|group",
            "Quantity": "1",
            "Currency": "JPY",
            "TotalAmount": ""
        }
    ]
}

Rules:
- Use 'N/A' for unknown values.
- Recognize Japanese receipts (税込, 税抜, 消費税, etc.) and extract tax information.
- For IC card transactions (Suica, PASMO, ICOCA), identify the card type.
- ReceiptID uses purchase date/time digits only. TicketID uses departure date/time digits plus carrier abbreviation.
- HotelID uses check-in date plus abbreviated hotel name. AttractionID uses visit date plus abbreviated attraction name.
- ItemID/SegmentID/DetailID increment per line item (e.g., ReceiptID-01, TicketID-02).
- Always include the arrays even if empty.
- For Japanese locations, include both Japanese name and romanized name when available.
"""

# Translate every non-Chinese value into zh_tw, using the format Chinese(non-Chinese).
json_translate_from_nonchinese_prompt = """\
This is a JSON representation of a travel document (receipt, ticket, hotel booking, or attraction ticket) from Japan.
Translate every non-Chinese value into Traditional Chinese (zh_tw) following these rules:

1. For amounts, prices, route numbers, times, dates, confirmation numbers, and other numerical or universally formatted values, keep them in their original format without translation.

2. For Japanese station names, airport names, hotel names, store names, and other location/business names:
   - Use the format: 中文(日文/English), e.g., "東京車站(東京駅)", "新宿(新宿)", "羽田機場(羽田空港)"
   - If the name already has kanji similar to Chinese, you may simplify, e.g., "東京駅" → "東京站(東京駅)"

3. For train/flight information:
   - CarrierName: 中文(日文), e.g., "新幹線(新幹線)", "JR東日本(JR東日本)"
   - RouteNumber: Keep as-is for train numbers, e.g., "のぞみ123号" → "希望號123(のぞみ123号)"

4. For Japanese food/product names on receipts:
   - Translate to Chinese with original in parentheses, e.g., "拉麵(ラーメン)", "便當(弁当)"

5. For categories and types, translate to Chinese:
   - shopping → 購物, restaurant → 餐廳, convenience → 便利商店, drugstore → 藥妝店, souvenir → 伴手禮
   - shinkansen → 新幹線, jr → JR線, metro → 地鐵, bus → 巴士

Return only the translated JSON while keeping the original structure and keys.
"""

# Validate required environment variables
for var, val in {
    "ChannelSecret": channel_secret,
    "ChannelAccessToken": channel_access_token,
    "AZURE_OPENAI_API_KEY": openai_api_key,
    "AZURE_OPENAI_MODEL_ENGINE": openai_model_engine,
    "GOOGLE_SHEETS_SPREADSHEET_ID": spreadsheet_id,
    "GOOGLE_APPLICATION_CREDENTIALS_JSON": credentials_json,
}.items():
    if val is None:
        logger.error(f"Specify {var} as environment variable.")
        sys.exit(1)

# Initialize Google Sheets storage
sheets_storage = SheetsStorage(spreadsheet_id, credentials_json)

# Initialize Line bot
app = FastAPI()
parser = WebhookParser(channel_secret)

# Initialize v3 MessagingApi for show_loading_animation (sync)
configuration = Configuration(access_token=channel_access_token)
api_client = ApiClient(configuration)
line_bot_api_v3 = MessagingApi(api_client)

# Initialize async API for async operations
async_api_client = AsyncApiClient(configuration)
async_line_bot_api = AsyncMessagingApi(async_api_client)
async_blob_api = AsyncMessagingApiBlob(async_api_client)

# Initialize Azure OpenAI client
openai_client = OpenAIModel(api_key=openai_api_key)


# ================= Azure OpenAI =================
def generate_aoai_text_complete(prompt: str) -> str:
    messages = [{"role": "user", "content": prompt}]
    success, res, err = openai_client.chat_completions(messages, openai_model_engine)
    if success and res:
        return res.get("choices", [{}])[0].get("message", {}).get("content", "")
    logger.error(f"Azure OpenAI error: {err}")
    return ""


def generate_json_from_receipt_image(img, prompt: str) -> str:
    buffered = BytesIO()
    img.save(buffered, format="JPEG")
    img_str = base64.b64encode(buffered.getvalue()).decode()
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {
                    "type": "image_url",
                    # "image_url": f"data:image/jpeg;base64,{img_str}"
                    "image_url": {
                        "url": f"data:image/jpeg;base64,{img_str}"
                    }
                },
            ],
        }
    ]
    success, res, err = openai_client.chat_completions(messages, openai_model_engine)
    if success and res:
        return res.get("choices", [{}])[0].get("message", {}).get("content", "")
    logger.error(f"Azure OpenAI error: {err}")
    return ""


# ================= Sheets Storage =================
def add_receipt(user_id: str, receipt_data: dict, items: list, image_path: str = None):
    try:
        receipt_id = receipt_data.get("ReceiptID")
        sheets_storage.store_receipt(user_id, receipt_data, items, image_path=image_path)
        logger.info(f"Add ReceiptID: {receipt_id} completed.")
    except Exception as e:
        logger.error(f"Error in add_receipt: {e}")


def add_ticket(user_id: str, ticket_data: dict, segments: list, image_path: str = None):
    try:
        ticket_id = ticket_data.get("TicketID")
        if not ticket_id:
            raise ValueError("TicketID 缺失，無法寫入資料庫")
        sheets_storage.store_ticket(user_id, ticket_data, segments, image_path=image_path)
        logger.info(f"Add TicketID: {ticket_id} completed.")
    except Exception as e:
        logger.error(f"Error in add_ticket: {e}")


def check_if_receipt_exists(user_id: str, receipt_id: str) -> bool:
    if not receipt_id:
        return False
    try:
        return sheets_storage.receipt_exists(user_id, receipt_id)
    except Exception as e:
        logger.error(f"Error in check_if_receipt_exists: {e}")
        return False


def check_if_ticket_exists(user_id: str, ticket_id: str) -> bool:
    if not ticket_id:
        return False
    try:
        return sheets_storage.ticket_exists(user_id, ticket_id)
    except Exception as e:
        logger.error(f"Error in check_if_ticket_exists: {e}")
        return False


def add_hotel(user_id: str, hotel_data: dict, room_details: list, image_path: str = None):
    try:
        hotel_id = hotel_data.get("HotelID")
        sheets_storage.store_hotel(user_id, hotel_data, room_details, image_path=image_path)
        logger.info(f"Add HotelID: {hotel_id} completed.")
    except Exception as e:
        logger.error(f"Error in add_hotel: {e}")


def add_attraction(user_id: str, attraction_data: dict, image_path: str = None):
    try:
        attraction_id = attraction_data.get("AttractionID")
        sheets_storage.store_attraction(user_id, attraction_data, image_path=image_path)
        logger.info(f"Add AttractionID: {attraction_id} completed.")
    except Exception as e:
        logger.error(f"Error in add_attraction: {e}")


def check_if_hotel_exists(user_id: str, hotel_id: str) -> bool:
    if not hotel_id:
        return False
    try:
        return sheets_storage.hotel_exists(user_id, hotel_id)
    except Exception as e:
        logger.error(f"Error in check_if_hotel_exists: {e}")
        return False


def check_if_attraction_exists(user_id: str, attraction_id: str) -> bool:
    if not attraction_id:
        return False
    try:
        return sheets_storage.attraction_exists(user_id, attraction_id)
    except Exception as e:
        logger.error(f"Error in check_if_attraction_exists: {e}")
        return False


# ================= Data Processing =================
def parse_receipt_json(receipt_json_str: str):
    try:
        # logger.debug(f"{receipt_json_str = }")
        json_str = receipt_json_str.strip()
        if json_str.startswith("```"):
            first_newline = json_str.find('\n')
            if first_newline != -1:
                json_str = json_str[first_newline + 1:]
            if json_str.endswith("```"):
                json_str = json_str[:json_str.rfind("```")].strip()
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            lines = [line for line in json_str.splitlines() if line.strip()]
            if not lines:
                raise ValueError("JSON 資料為空")
            if not lines[0].strip().startswith('{'):
                lines[0] = '{' + lines[0]
            if not lines[-1].strip().endswith('}'):
                lines[-1] = lines[-1] + '}'
            compact_json = ' '.join(lines)
            logger.debug(f"{compact_json = }")
            return json.loads(compact_json)
    except json.JSONDecodeError as e:
        logger.error(f"JSONDecodeError: {e.msg} at line {e.lineno}, column {e.colno}")
        logger.error(f"{receipt_json_str = }")
    except Exception as e:
        logger.error(f"Error parsing JSON: {e}")
    return None


def extract_receipt_data(receipt_json_obj: dict):
    receipt_obj = None
    items = []
    if receipt_json_obj:
        receipt_obj = receipt_json_obj.get("Receipt")
        if receipt_obj and isinstance(receipt_obj, list):
            receipt_obj = receipt_obj[0]
        items = receipt_json_obj.get("Items") or []
    return items, receipt_obj


def extract_ticket_data(receipt_json_obj: dict):
    ticket_obj = None
    segments = []
    if receipt_json_obj:
        ticket_obj = receipt_json_obj.get("Ticket")
        if ticket_obj and isinstance(ticket_obj, list):
            ticket_obj = ticket_obj[0]
        segments = receipt_json_obj.get("Segments") or []
    return segments, ticket_obj


def extract_hotel_data(json_obj: dict):
    hotel_obj = None
    room_details = []
    if json_obj:
        hotel_obj = json_obj.get("Hotel")
        if hotel_obj and isinstance(hotel_obj, list):
            hotel_obj = hotel_obj[0]
        room_details = json_obj.get("RoomDetails") or []
    return room_details, hotel_obj


def extract_attraction_data(json_obj: dict):
    attraction_obj = None
    if json_obj:
        attraction_obj = json_obj.get("Attraction")
        if attraction_obj and isinstance(attraction_obj, list):
            attraction_obj = attraction_obj[0]
    return attraction_obj


# ================= Quick Reply =================
def get_quick_reply_buttons() -> QuickReply:
    """Generate Quick Reply buttons for common actions."""
    return QuickReply(items=[
        QuickReplyItem(action=MessageAction(label="📊 統計費用", text="!統計")),
        QuickReplyItem(action=MessageAction(label="🚄 交通行程", text="!行程")),
        QuickReplyItem(action=MessageAction(label="🏨 住宿清單", text="!住宿")),
        QuickReplyItem(action=MessageAction(label="🎫 景點紀錄", text="!景點")),
        QuickReplyItem(action=MessageAction(label="❓ 幫助說明", text="!幫助")),
    ])


# ================= Flex Message =================
def get_receipt_flex_msg(receipt_data: dict, items: list) -> FlexMessage:
    items_contents = [
        {
            "type": "box",
            "layout": "horizontal",
            "contents": [
                {"type": "text", "text": f"{item.get('ItemName')}", "size": "sm", "color": "#555555", "flex": 0},
                {"type": "text", "text": f"${item.get('ItemPrice')}", "size": "sm", "color": "#111111", "align": "end"},
            ],
        }
        for item in items
    ]

    flex_msg = {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {"type": "text", "text": "收據明細", "weight": "bold", "color": "#1DB446", "size": "sm"},
                {"type": "text", "text": f"{receipt_data.get('PurchaseStore')}", "weight": "bold", "size": "xxl", "margin": "md"},
                {"type": "text", "text": f"{receipt_data.get('PurchaseAddress')}", "size": "xs", "color": "#aaaaaa", "wrap": True},
                {"type": "separator", "margin": "xxl"},
                {"type": "box", "layout": "vertical", "margin": "xxl", "spacing": "sm", "contents": items_contents},
                {"type": "separator", "margin": "xxl"},
                {
                    "type": "box",
                    "layout": "horizontal",
                    "margin": "md",
                    "contents": [
                        {"type": "text", "text": "收據 ID", "size": "xs", "color": "#aaaaaa", "flex": 0},
                        {"type": "text", "text": f"{receipt_data.get('ReceiptID')}", "color": "#aaaaaa", "size": "xs", "align": "end"},
                    ],
                },
            ],
        },
        "styles": {"footer": {"separator": True}},
    }
    return FlexMessage(altText="Receipt Data", contents=FlexContainer.from_dict(flex_msg))


def get_train_ticket_flex_msg(ticket_data: dict, items: list) -> FlexMessage:
    segment_contents = [
        {
            "type": "box",
            "layout": "vertical",
            "margin": "md",
            "spacing": "xs",
            "contents": [
                {"type": "text", "text": f"{item.get('SegmentName', 'Segment')}", "size": "sm", "weight": "bold", "color": "#1DB446"},
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "From", "size": "xs", "color": "#aaaaaa", "flex": 0},
                        {"type": "text", "text": f"{item.get('Departure', 'N/A')}", "size": "xs", "color": "#111111", "align": "end"},
                    ],
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "To", "size": "xs", "color": "#aaaaaa", "flex": 0},
                        {"type": "text", "text": f"{item.get('Arrival', 'N/A')}", "size": "xs", "color": "#111111", "align": "end"},
                    ],
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "Seat", "size": "xs", "color": "#aaaaaa", "flex": 0},
                        {"type": "text", "text": f"{item.get('Seat', 'N/A')}", "size": "xs", "color": "#111111", "align": "end"},
                    ],
                },
            ],
        }
        for item in items
    ]
    if not segment_contents:
        segment_contents = [{"type": "text", "text": "No segment data", "size": "xs", "color": "#aaaaaa"}]

    flex_msg = {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "票券資訊", "weight": "bold", "color": "#1DB446", "size": "sm"},
                {"type": "text", "text": f"{ticket_data.get('CarrierName', 'N/A')}", "weight": "bold", "size": "xl"},
                {
                    "type": "box",
                    "layout": "horizontal",
                    "margin": "sm",
                    "contents": [
                        {"type": "text", "text": "車次/航班", "size": "xs", "color": "#aaaaaa", "flex": 0},
                        {"type": "text", "text": f"{ticket_data.get('RouteNumber', 'N/A')}", "size": "xs", "color": "#111111", "align": "end"},
                    ],
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "出發", "size": "xs", "color": "#aaaaaa", "flex": 0},
                        {"type": "text", "text": f"{ticket_data.get('DepartureTime', 'N/A')} {ticket_data.get('DepartureStation', '')}".strip(), "size": "xs", "color": "#111111",
                         "align": "end"},
                    ],
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "抵達", "size": "xs", "color": "#aaaaaa", "flex": 0},
                        {"type": "text", "text": f"{ticket_data.get('ArrivalTime', 'N/A')} {ticket_data.get('ArrivalStation', '')}".strip(), "size": "xs", "color": "#111111",
                         "align": "end"},
                    ],
                },
                {"type": "separator", "margin": "md"},
                {"type": "box", "layout": "vertical", "margin": "md", "contents": segment_contents},
                {"type": "separator", "margin": "md"},
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "乘客", "size": "xs", "color": "#aaaaaa", "flex": 0},
                        {"type": "text", "text": f"{ticket_data.get('PassengerName', 'N/A')}", "size": "xs", "color": "#111111", "align": "end"},
                    ],
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "票號", "size": "xs", "color": "#aaaaaa", "flex": 0},
                        {"type": "text", "text": f"{ticket_data.get('TicketID', 'N/A')}", "size": "xs", "color": "#111111", "align": "end"},
                    ],
                },
            ],
        },
        "styles": {"footer": {"separator": True}},
    }
    return FlexMessage(altText="Ticket Detail", contents=FlexContainer.from_dict(flex_msg))


def get_hotel_flex_msg(hotel_data: dict, room_details: list) -> FlexMessage:
    detail_contents = [
        {
            "type": "box",
            "layout": "horizontal",
            "contents": [
                {"type": "text", "text": f"{detail.get('Description', 'N/A')}", "size": "sm", "color": "#555555", "flex": 0},
                {"type": "text", "text": f"¥{detail.get('Price', 'N/A')}", "size": "sm", "color": "#111111", "align": "end"},
            ],
        }
        for detail in room_details
    ]
    if not detail_contents:
        detail_contents = [{"type": "text", "text": "無明細資料", "size": "xs", "color": "#aaaaaa"}]

    flex_msg = {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "🏨 住宿資訊", "weight": "bold", "color": "#1DB446", "size": "sm"},
                {"type": "text", "text": f"{hotel_data.get('HotelName', 'N/A')}", "weight": "bold", "size": "xl", "wrap": True},
                {"type": "text", "text": f"{hotel_data.get('HotelAddress', 'N/A')}", "size": "xs", "color": "#aaaaaa", "wrap": True},
                {"type": "separator", "margin": "md"},
                {
                    "type": "box",
                    "layout": "horizontal",
                    "margin": "md",
                    "contents": [
                        {"type": "text", "text": "入住", "size": "xs", "color": "#aaaaaa", "flex": 0},
                        {"type": "text", "text": f"{hotel_data.get('CheckInDate', 'N/A')}", "size": "xs", "color": "#111111", "align": "end"},
                    ],
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "退房", "size": "xs", "color": "#aaaaaa", "flex": 0},
                        {"type": "text", "text": f"{hotel_data.get('CheckOutDate', 'N/A')}", "size": "xs", "color": "#111111", "align": "end"},
                    ],
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "晚數", "size": "xs", "color": "#aaaaaa", "flex": 0},
                        {"type": "text", "text": f"{hotel_data.get('Nights', 'N/A')} 晚", "size": "xs", "color": "#111111", "align": "end"},
                    ],
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "房型", "size": "xs", "color": "#aaaaaa", "flex": 0},
                        {"type": "text", "text": f"{hotel_data.get('RoomType', 'N/A')}", "size": "xs", "color": "#111111", "align": "end"},
                    ],
                },
                {"type": "separator", "margin": "md"},
                {"type": "box", "layout": "vertical", "margin": "md", "spacing": "sm", "contents": detail_contents},
                {"type": "separator", "margin": "md"},
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "總金額", "size": "sm", "color": "#555555", "flex": 0, "weight": "bold"},
                        {"type": "text", "text": f"¥{hotel_data.get('TotalAmount', 'N/A')}", "size": "sm", "color": "#1DB446", "align": "end", "weight": "bold"},
                    ],
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "margin": "sm",
                    "contents": [
                        {"type": "text", "text": "確認碼", "size": "xs", "color": "#aaaaaa", "flex": 0},
                        {"type": "text", "text": f"{hotel_data.get('ConfirmationNumber', 'N/A')}", "size": "xs", "color": "#111111", "align": "end"},
                    ],
                },
            ],
        },
        "styles": {"footer": {"separator": True}},
    }
    return FlexMessage(altText="Hotel Booking", contents=FlexContainer.from_dict(flex_msg))


def get_attraction_flex_msg(attraction_data: dict) -> FlexMessage:
    flex_msg = {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "🎫 景點門票", "weight": "bold", "color": "#1DB446", "size": "sm"},
                {"type": "text", "text": f"{attraction_data.get('AttractionName', 'N/A')}", "weight": "bold", "size": "xl", "wrap": True},
                {"type": "text", "text": f"{attraction_data.get('AttractionAddress', 'N/A')}", "size": "xs", "color": "#aaaaaa", "wrap": True},
                {"type": "separator", "margin": "md"},
                {
                    "type": "box",
                    "layout": "horizontal",
                    "margin": "md",
                    "contents": [
                        {"type": "text", "text": "參觀日期", "size": "xs", "color": "#aaaaaa", "flex": 0},
                        {"type": "text", "text": f"{attraction_data.get('VisitDate', 'N/A')}", "size": "xs", "color": "#111111", "align": "end"},
                    ],
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "時間", "size": "xs", "color": "#aaaaaa", "flex": 0},
                        {"type": "text", "text": f"{attraction_data.get('VisitTime', 'N/A')}", "size": "xs", "color": "#111111", "align": "end"},
                    ],
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "票種", "size": "xs", "color": "#aaaaaa", "flex": 0},
                        {"type": "text", "text": f"{attraction_data.get('TicketType', 'N/A')}", "size": "xs", "color": "#111111", "align": "end"},
                    ],
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "數量", "size": "xs", "color": "#aaaaaa", "flex": 0},
                        {"type": "text", "text": f"{attraction_data.get('Quantity', '1')} 張", "size": "xs", "color": "#111111", "align": "end"},
                    ],
                },
                {"type": "separator", "margin": "md"},
                {
                    "type": "box",
                    "layout": "horizontal",
                    "margin": "md",
                    "contents": [
                        {"type": "text", "text": "總金額", "size": "sm", "color": "#555555", "flex": 0, "weight": "bold"},
                        {"type": "text", "text": f"¥{attraction_data.get('TotalAmount', 'N/A')}", "size": "sm", "color": "#1DB446", "align": "end", "weight": "bold"},
                    ],
                },
            ],
        },
        "styles": {"footer": {"separator": True}},
    }
    return FlexMessage(altText="Attraction Ticket", contents=FlexContainer.from_dict(flex_msg))


# ================= Main Flow =================
@app.post("/callback")
async def handle_callback(request: Request):
    signature = request.headers["X-Line-Signature"]
    body = (await request.body()).decode()
    try:
        events = parser.parse(body, signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    for event in events:
        if not isinstance(event, MessageEvent):
            continue
        user_id = event.source.user_id
        try:
            # Show loading animation
            show_loading_animation_request = ShowLoadingAnimationRequest(chatId=user_id)
            # show_loading_animation_request = ShowLoadingAnimationRequest(chatId=user_id, loadingSeconds=5)
            api_response = line_bot_api_v3.show_loading_animation(show_loading_animation_request)
            # logger.debug(f"{api_response = }")
        except Exception as e:
            logger.warning(f"Exception when calling MessagingApi->show_loading_animation: {e}")

        if event.message.type == "text":
            text = event.message.text.strip()
            logger.info(f'{user_id}: [{openai_model_engine}]{text}')
            all_receipts = sheets_storage.get_user_snapshot(user_id)
            reply_msg = TextMessage(text="No message to reply with")
            if text == "!清空":
                reply_msg = TextMessage(text="對話歷史紀錄已經清空！", quickReply=get_quick_reply_buttons())
                sheets_storage.clear_user_data(user_id)
            elif text == "!幫助" or text == "!help":
                help_text = """📱 旅遊小幫手使用說明

📸 拍照上傳：
• 購物收據 - 自動識別商品明細
• 車票/機票 - 記錄交通行程
• 住宿確認單 - 記錄住宿資訊
• 景點門票 - 記錄參觀紀錄

💬 文字指令：
• !統計 - 查看旅費總計
• !行程 - 查看交通行程
• !住宿 - 查看住宿清單
• !景點 - 查看景點紀錄
• !清空 - 清除所有紀錄
• !幫助 - 顯示此說明

🤖 智慧問答：
直接輸入問題，例如：
• 「今天花了多少錢？」
• 「最貴的一筆消費是什麼？」
• 「明天的行程是什麼？」"""
                reply_msg = TextMessage(text=help_text, quickReply=get_quick_reply_buttons())
            elif text == "!統計":
                prompt_msg = (
                    f"This is my travel expense data: {all_receipts}; "
                    "Please calculate and summarize:\n"
                    "1. Total spending on receipts/shopping (總購物消費)\n"
                    "2. Total spending on transportation (總交通費用)\n"
                    "3. Total spending on accommodation (總住宿費用)\n"
                    "4. Total spending on attractions (總景點門票)\n"
                    "5. Grand total (旅費總計)\n"
                    "6. Daily average (日均花費)\n"
                    "Format nicely with emojis. Reply in zh_tw."
                )
                response_text = generate_aoai_text_complete(prompt_msg)
                reply_msg = TextMessage(text=response_text, quickReply=get_quick_reply_buttons())
            elif text == "!行程":
                prompt_msg = (
                    f"This is my travel data: {all_receipts}; "
                    "Please list all my transportation records in chronological order. "
                    "Include: date, time, carrier/train name, route (from -> to), seat info. "
                    "Format as a clean timeline. Reply in zh_tw."
                )
                response_text = generate_aoai_text_complete(prompt_msg)
                reply_msg = TextMessage(text=response_text, quickReply=get_quick_reply_buttons())
            elif text == "!住宿":
                prompt_msg = (
                    f"This is my travel data: {all_receipts}; "
                    "Please list all my hotel/accommodation bookings in chronological order. "
                    "Include: hotel name, address, check-in/check-out dates, nights, room type, total amount. "
                    "Format nicely. Reply in zh_tw."
                )
                response_text = generate_aoai_text_complete(prompt_msg)
                reply_msg = TextMessage(text=response_text, quickReply=get_quick_reply_buttons())
            elif text == "!景點":
                prompt_msg = (
                    f"This is my travel data: {all_receipts}; "
                    "Please list all my attraction/sightseeing records in chronological order. "
                    "Include: attraction name, visit date/time, ticket type, quantity, amount. "
                    "Format nicely. Reply in zh_tw."
                )
                response_text = generate_aoai_text_complete(prompt_msg)
                reply_msg = TextMessage(text=response_text, quickReply=get_quick_reply_buttons())
            else:
                prompt_msg = (
                    f"You are a helpful travel assistant for a trip to Japan (Tokyo and Hokkaido, 12 days). "
                    f"Here is my entire travel data including receipts, tickets, hotels, and attractions: {all_receipts}; "
                    f"Please answer my question based on this information. If the question is about local recommendations, "
                    f"you can provide suggestions based on common knowledge about Japan travel. "
                    f"Question: {text}. Reply in zh_tw."
                )
                response_text = generate_aoai_text_complete(prompt_msg)
                reply_msg = TextMessage(text=response_text, quickReply=get_quick_reply_buttons())
            logger.info(f'{user_id}: [{openai_model_engine}]' + text)
            await async_line_bot_api.reply_message(ReplyMessageRequest(replyToken=event.reply_token, messages=[reply_msg]))
        elif event.message.type == "image":
            message_content = await async_blob_api.get_message_content(event.message.id)
            img = PIL.Image.open(BytesIO(message_content))

            # Save image to local file for uploading to Google Drive
            input_image_path = f'{str(uuid.uuid4())}.jpg'
            img.save(input_image_path, format='JPEG')

            logger.info(f'{user_id}: [{openai_model_engine}] {event.message.type} received')
            result_text = generate_json_from_receipt_image(img, image_prompt)
            if not result_text:
                logger.warning("模型沒有回傳任何資料")
                await async_line_bot_api.reply_message(ReplyMessageRequest(replyToken=event.reply_token, messages=[TextMessage(text="未取得辨識結果，請稍後再試")]))
                # Clean up temp image file
                if os.path.exists(input_image_path):
                    os.remove(input_image_path)
                return "OK"
            # logger.info(f'{user_id}: [{openai_model_engine}] Before Translate Result: {result_text}')
            tw_result_text = generate_aoai_text_complete(
                result_text + "\n --- " + json_translate_from_nonchinese_prompt
            )
            logger.info(f'{user_id}: [{openai_model_engine}] After Translate Result: {tw_result_text}')
            parsed_result = parse_receipt_json(result_text)
            if parsed_result is None:
                await async_line_bot_api.reply_message(ReplyMessageRequest(replyToken=event.reply_token, messages=[TextMessage(text="資料解析失敗，請確認影像內容")]))
                # Clean up temp image file
                if os.path.exists(input_image_path):
                    os.remove(input_image_path)
                return "OK"
            parsed_tw_result = parse_receipt_json(tw_result_text)
            if parsed_tw_result is None:
                await async_line_bot_api.reply_message(ReplyMessageRequest(replyToken=event.reply_token, messages=[TextMessage(text="翻譯後資料解析失敗，請稍後再試")]))
                # Clean up temp image file
                if os.path.exists(input_image_path):
                    os.remove(input_image_path)
                return "OK"
            items, receipt = extract_receipt_data(parsed_result)
            segments, ticket = extract_ticket_data(parsed_result)
            room_details, hotel = extract_hotel_data(parsed_result)
            attraction = extract_attraction_data(parsed_result)
            tw_items, tw_receipt = extract_receipt_data(parsed_tw_result)
            tw_segments, tw_ticket = extract_ticket_data(parsed_tw_result)
            tw_room_details, tw_hotel = extract_hotel_data(parsed_tw_result)
            tw_attraction = extract_attraction_data(parsed_tw_result)
            if receipt:
                if tw_receipt is None:
                    logger.warning("翻譯後收據資料解析失敗")
                    await async_line_bot_api.reply_message(ReplyMessageRequest(replyToken=event.reply_token, messages=[TextMessage(text="收據資料解析失敗，請檢查圖片或資料格式")]))
                    # Clean up temp image file
                    if os.path.exists(input_image_path):
                        os.remove(input_image_path)
                    return "OK"
                receipt_id = receipt.get("ReceiptID")
                reply_messages = []
                if check_if_receipt_exists(user_id, receipt_id):
                    reply_messages.append(TextMessage(text="這個收據已經存在資料庫中。"))
                else:
                    add_receipt(user_id, tw_receipt, tw_items, image_path=input_image_path)
                reply_msg = get_receipt_flex_msg(receipt, items)
                chinese_reply_msg = get_receipt_flex_msg(tw_receipt, tw_items)
                # reply_messages.append(reply_msg)
                reply_messages.append(chinese_reply_msg)
                logger.info(f'{user_id}: [{openai_model_engine}] {receipt_id} Receipt processed and reply sent')
                await async_line_bot_api.reply_message(ReplyMessageRequest(replyToken=event.reply_token, messages=reply_messages))
                # Clean up temp image file
                if os.path.exists(input_image_path):
                    os.remove(input_image_path)
                return "OK"
            if ticket:
                if tw_ticket is None:
                    logger.warning("翻譯後票券資料解析失敗")
                    await async_line_bot_api.reply_message(ReplyMessageRequest(replyToken=event.reply_token, messages=[TextMessage(text="票券資料解析失敗，請檢查圖片或資料格式")]))
                    # Clean up temp image file
                    if os.path.exists(input_image_path):
                        os.remove(input_image_path)
                    return "OK"
                ticket_id = ticket.get("TicketID")
                if not ticket_id:
                    logger.warning("票券缺少 TicketID")
                    await async_line_bot_api.reply_message(ReplyMessageRequest(replyToken=event.reply_token, messages=[TextMessage(text="票券缺少票號，請重新拍攝或輸入")]))
                    # Clean up temp image file
                    if os.path.exists(input_image_path):
                        os.remove(input_image_path)
                    return "OK"
                reply_messages = []
                if check_if_ticket_exists(user_id, ticket_id):
                    reply_messages.append(TextMessage(text="這張票券已經存在資料庫中。"))
                else:
                    add_ticket(user_id, tw_ticket, tw_segments, image_path=input_image_path)
                reply_msg = get_train_ticket_flex_msg(ticket, segments)
                chinese_reply_msg = get_train_ticket_flex_msg(tw_ticket, tw_segments)
                # reply_messages.append(reply_msg)
                reply_messages.append(chinese_reply_msg)
                logger.info(f'{user_id}: [{openai_model_engine}]{ticket_id} Ticket processed and reply sent')
                await async_line_bot_api.reply_message(ReplyMessageRequest(replyToken=event.reply_token, messages=reply_messages))
                # Clean up temp image file
                if os.path.exists(input_image_path):
                    os.remove(input_image_path)
                return "OK"
            if hotel:
                if tw_hotel is None:
                    logger.warning("翻譯後住宿資料解析失敗")
                    await async_line_bot_api.reply_message(ReplyMessageRequest(replyToken=event.reply_token, messages=[TextMessage(text="住宿資料解析失敗，請檢查圖片或資料格式")]))
                    if os.path.exists(input_image_path):
                        os.remove(input_image_path)
                    return "OK"
                hotel_id = hotel.get("HotelID")
                if not hotel_id:
                    logger.warning("住宿缺少 HotelID")
                    await async_line_bot_api.reply_message(ReplyMessageRequest(replyToken=event.reply_token, messages=[TextMessage(text="住宿缺少識別碼，請重新拍攝或輸入")]))
                    if os.path.exists(input_image_path):
                        os.remove(input_image_path)
                    return "OK"
                reply_messages = []
                if check_if_hotel_exists(user_id, hotel_id):
                    reply_messages.append(TextMessage(text="這筆住宿紀錄已經存在資料庫中。"))
                else:
                    add_hotel(user_id, tw_hotel, tw_room_details, image_path=input_image_path)
                chinese_reply_msg = get_hotel_flex_msg(tw_hotel, tw_room_details)
                reply_messages.append(chinese_reply_msg)
                logger.info(f'{user_id}: [{openai_model_engine}]{hotel_id} Hotel processed and reply sent')
                await async_line_bot_api.reply_message(ReplyMessageRequest(replyToken=event.reply_token, messages=reply_messages))
                if os.path.exists(input_image_path):
                    os.remove(input_image_path)
                return "OK"
            if attraction:
                if tw_attraction is None:
                    logger.warning("翻譯後景點門票資料解析失敗")
                    await async_line_bot_api.reply_message(ReplyMessageRequest(replyToken=event.reply_token, messages=[TextMessage(text="景點門票資料解析失敗，請檢查圖片或資料格式")]))
                    if os.path.exists(input_image_path):
                        os.remove(input_image_path)
                    return "OK"
                attraction_id = attraction.get("AttractionID")
                if not attraction_id:
                    logger.warning("景點門票缺少 AttractionID")
                    await async_line_bot_api.reply_message(ReplyMessageRequest(replyToken=event.reply_token, messages=[TextMessage(text="景點門票缺少識別碼，請重新拍攝或輸入")]))
                    if os.path.exists(input_image_path):
                        os.remove(input_image_path)
                    return "OK"
                reply_messages = []
                if check_if_attraction_exists(user_id, attraction_id):
                    reply_messages.append(TextMessage(text="這張景點門票已經存在資料庫中。"))
                else:
                    add_attraction(user_id, tw_attraction, image_path=input_image_path)
                chinese_reply_msg = get_attraction_flex_msg(tw_attraction)
                reply_messages.append(chinese_reply_msg)
                logger.info(f'{user_id}: [{openai_model_engine}]{attraction_id} Attraction processed and reply sent')
                await async_line_bot_api.reply_message(ReplyMessageRequest(replyToken=event.reply_token, messages=reply_messages))
                if os.path.exists(input_image_path):
                    os.remove(input_image_path)
                return "OK"
            # Clean up temp image file if no recognized document type
            await async_line_bot_api.reply_message(ReplyMessageRequest(replyToken=event.reply_token, messages=[TextMessage(text="無法識別文件類型，請確認是否為收據、車票、住宿確認單或景點門票")]))
            if os.path.exists(input_image_path):
                os.remove(input_image_path)
    return None


@app.get("/")
def home():
    return "Hello World"


if __name__ == "__main__":
    # import uvicorn
    # port = int(os.environ.get('PORT', 5000))
    # uvicorn.run(app, host='0.0.0.0', port=port)
    pass
