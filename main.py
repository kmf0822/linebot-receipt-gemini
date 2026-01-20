import base64
import json
import os
import sys
from io import BytesIO

import PIL.Image
import aiohttp
import firebase_admin
from fastapi import FastAPI, HTTPException, Request
from firebase_admin import credentials, db
from linebot import AsyncLineBotApi, WebhookParser
from linebot.aiohttp_async_http_client import AiohttpAsyncHttpClient
from linebot.exceptions import InvalidSignatureError
from linebot.models import FlexSendMessage, MessageEvent, TextSendMessage

from models import OpenAIModel
from src.logger import logger

# Environment variables
channel_secret = os.getenv("ChannelSecret")
channel_access_token = os.getenv("ChannelAccessToken")
openai_api_key = os.getenv("AZURE_OPENAI_API_KEY")
openai_model_engine = os.getenv("AZURE_OPENAI_MODEL_ENGINE")
firebase_url = os.getenv("FIREBASE_URL")

image_prompt = """\
You are a meticulous travel secretary analyzing either a shopping receipt or a transportation ticket (train or flight).
Return exactly one JSON object following one of the schemas below and no additional narration.

Receipt schema:
{
    "Receipt": [
        {
            "ReceiptID": "YYYYMMDDHHmm",
            "PurchaseStore": "",
            "PurchaseDate": "YYYY/MM/DD HH:mm",
            "PurchaseAddress": "",
            "TotalAmount": ""
        }
    ],
    "Items": [
        {"ItemID": "ReceiptID-01", "ReceiptID": "ReceiptID", "ItemName": "", "ItemPrice": ""}
    ]
}

Ticket schema:
{
    "Ticket": [
        {
            "TicketID": "YYYYMMDDHHmmCarrier",
            "CarrierName": "",
            "RouteNumber": "",
            "TicketType": "train|flight",
            "DepartureStation": "",
            "ArrivalStation": "",
            "DepartureTime": "YYYY/MM/DD HH:mm",
            "ArrivalTime": "YYYY/MM/DD HH:mm",
            "PassengerName": "",
            "TotalAmount": ""
        }
    ],
    "Segments": [
        {"SegmentID": "TicketID-01", "SegmentName": "", "Departure": "", "Arrival": "", "Seat": ""}
    ]
}

Rules:
- Use 'N/A' for unknown values.
- ReceiptID uses purchase date/time digits only. TicketID uses departure date/time digits plus carrier abbreviation.
- ItemID/SegmentID increment per line item (e.g., ReceiptID-01, TicketID-02).
- Always include the arrays even if empty.
"""

json_translate_from_nonchinese_prompt = """\
This is a JSON representation of a receipt or travel ticket.
Translate every non-Chinese value into zh_tw, using the format non-Chinese(Chinese).
Return only the translated JSON while keeping the original structure and keys.
"""

# Validate required environment variables
for var, val in {
    "ChannelSecret": channel_secret,
    "ChannelAccessToken": channel_access_token,
    "AZURE_OPENAI_API_KEY": openai_api_key,
    "AZURE_OPENAI_MODEL_ENGINE": openai_model_engine,
    "FIREBASE_URL": firebase_url,
}.items():
    if val is None:
        logger.error(f"Specify {var} as environment variable.")
        sys.exit(1)

# Initialize Firebase Admin
# cred = credentials.ApplicationDefault()
cred_info = json.loads(os.environ['GOOGLE_APPLICATION_CREDENTIALS_JSON'])
cred = credentials.Certificate(cred_info)
if not firebase_admin._apps:
    firebase_admin.initialize_app(cred, {"databaseURL": firebase_url})

# Initialize Line bot
app = FastAPI()
session = aiohttp.ClientSession()
async_http_client = AiohttpAsyncHttpClient(session)
line_bot_api = AsyncLineBotApi(channel_access_token, async_http_client)
parser = WebhookParser(channel_secret)

# Initialize Azure OpenAI client
openai_client = OpenAIModel(api_key=openai_api_key)
os.environ["OPENAI_MODEL_ENGINE"] = openai_model_engine


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


# ================= Firebase =================
def add_receipt(receipt_data: dict, items: list, user_receipt_path: str, user_item_path: str):
    try:
        receipt_id = receipt_data.get("ReceiptID")
        db.reference(user_receipt_path).child(receipt_id).set(receipt_data)
        for item in items:
            item_id = item.get("ItemID")
            db.reference(user_item_path).child(item_id).set(item)
        logger.info(f"Add ReceiptID: {receipt_id} completed.")
    except Exception as e:
        logger.error(f"Error in add_receipt: {e}")


def add_ticket(ticket_data: dict, segments: list, user_ticket_path: str, user_segment_path: str):
    try:
        ticket_id = ticket_data.get("TicketID")
        if not ticket_id:
            raise ValueError("TicketID 缺失，無法寫入資料庫")
        db.reference(user_ticket_path).child(ticket_id).set(ticket_data)
        for idx, segment in enumerate(segments or [], start=1):
            if not isinstance(segment, dict):
                continue
            segment_id = segment.get("SegmentID") or f"{ticket_id}-{str(idx).zfill(2)}"
            segment["SegmentID"] = segment_id
            db.reference(user_segment_path).child(segment_id).set(segment)
        logger.info(f"Add TicketID: {ticket_id} completed.")
    except Exception as e:
        logger.error(f"Error in add_ticket: {e}")


def check_if_receipt_exists(receipt_id: str, user_receipt_path: str) -> bool:
    try:
        receipt = db.reference(user_receipt_path).child(receipt_id).get()
        return receipt is not None
    except Exception as e:
        logger.error(f"Error in check_if_receipt_exists: {e}")
        return False


def check_if_ticket_exists(ticket_id: str, user_ticket_path: str) -> bool:
    if not ticket_id:
        return False
    try:
        ticket = db.reference(user_ticket_path).child(ticket_id).get()
        return ticket is not None
    except Exception as e:
        logger.error(f"Error in check_if_ticket_exists: {e}")
        return False


# ================= Data Processing =================
def parse_receipt_json(receipt_json_str: str):
    try:
        logger.debug(f"{receipt_json_str = }")
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


# ================= Flex Message =================
def get_receipt_flex_msg(receipt_data: dict, items: list) -> FlexSendMessage:
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
                {"type": "text", "text": "收據OCR明細", "weight": "bold", "color": "#1DB446", "size": "sm"},
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
    return FlexSendMessage(alt_text="Receipt Data", contents=flex_msg)


def get_train_ticket_flex_msg(ticket_data: dict, items: list) -> FlexSendMessage:
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
    return FlexSendMessage(alt_text="Ticket Detail", contents=flex_msg)


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
        user_receipt_path = f"receipt_helper/{user_id}/Receipts"
        user_item_path = f"receipt_helper/{user_id}/Items"
        user_all_receipts_path = f"receipt_helper/{user_id}"

        if event.message.type == "text":
            user_id = event.source.user_id
            text = event.message.text.strip()
            logger.info(f'{user_id}: [{openai_model_engine}]{text}')
            all_receipts = db.reference(user_all_receipts_path).get()
            reply_msg = TextSendMessage(text="No message to reply with")
            msg = event.message.text
            if msg == "!清空":
                reply_msg = TextSendMessage(text="對話歷史紀錄已經清空！")
                db.reference(user_all_receipts_path).delete()
            else:
                prompt_msg = (
                    f"Here is my entire receipt list during my travel: {all_receipts}; "
                    f"please answer my question based on this information. {msg}. Reply in zh_tw."
                )
                response_text = generate_aoai_text_complete(prompt_msg)
                reply_msg = TextSendMessage(text=response_text)
            logger.info(f'{user_id}: [{os.getenv("OPENAI_MODEL_ENGINE")}]' + msg)
            await line_bot_api.reply_message(event.reply_token, reply_msg)
        elif event.message.type == "image":
            message_content = await line_bot_api.get_message_content(event.message.id)
            image_content = b""
            async for s in message_content.iter_content():
                image_content += s
            img = PIL.Image.open(BytesIO(image_content))
            result_text = generate_json_from_receipt_image(img, image_prompt)
            if not result_text:
                logger.warning("模型沒有回傳任何資料")
                await line_bot_api.reply_message(event.reply_token, TextSendMessage(text="未取得辨識結果，請稍後再試"))
                return "OK"
            logger.info(f'{user_id}: [{os.getenv("OPENAI_MODEL_ENGINE")}] Before Translate Result: {result_text}')
            tw_result_text = generate_aoai_text_complete(
                result_text + "\n --- " + json_translate_from_nonchinese_prompt
            )
            logger.info(f'{user_id}: [{os.getenv("OPENAI_MODEL_ENGINE")}]{tw_result_text} After Translate Result: {tw_result_text}')
            parsed_result = parse_receipt_json(result_text)
            if parsed_result is None:
                await line_bot_api.reply_message(event.reply_token, TextSendMessage(text="資料解析失敗，請確認影像內容"))
                return "OK"
            parsed_tw_result = parse_receipt_json(tw_result_text)
            if parsed_tw_result is None:
                await line_bot_api.reply_message(event.reply_token, TextSendMessage(text="翻譯後資料解析失敗，請稍後再試"))
                return "OK"
            items, receipt = extract_receipt_data(parsed_result)
            segments, ticket = extract_ticket_data(parsed_result)
            tw_items, tw_receipt = extract_receipt_data(parsed_tw_result)
            tw_segments, tw_ticket = extract_ticket_data(parsed_tw_result)
            user_ticket_path = f"receipt_helper/{user_id}/Tickets"
            user_segment_path = f"receipt_helper/{user_id}/Segments"
            if receipt:
                if tw_receipt is None:
                    logger.warning("翻譯後收據資料解析失敗")
                    await line_bot_api.reply_message(event.reply_token, TextSendMessage(text="收據資料解析失敗，請檢查圖片或資料格式"))
                    return "OK"
                receipt_id = receipt.get("ReceiptID")
                if check_if_receipt_exists(receipt_id, user_receipt_path):
                    reply_msg = get_receipt_flex_msg(receipt, items)
                    chinese_reply_msg = get_receipt_flex_msg(tw_receipt, tw_items)
                    await line_bot_api.reply_message(
                        event.reply_token,
                        [TextSendMessage(text="這個收據已經存在資料庫中。"), reply_msg, chinese_reply_msg],
                    )
                    return "OK"
                add_receipt(tw_receipt, tw_items, user_receipt_path, user_item_path)
                reply_msg = get_receipt_flex_msg(receipt, items)
                chinese_reply_msg = get_receipt_flex_msg(tw_receipt, tw_items)
                await line_bot_api.reply_message(event.reply_token, [reply_msg, chinese_reply_msg])
                return "OK"
            if ticket:
                if tw_ticket is None:
                    logger.warning("翻譯後票券資料解析失敗")
                    await line_bot_api.reply_message(event.reply_token, TextSendMessage(text="票券資料解析失敗，請檢查圖片或資料格式"))
                    return "OK"
                ticket_id = ticket.get("TicketID")
                if not ticket_id:
                    logger.warning("票券缺少 TicketID")
                    await line_bot_api.reply_message(event.reply_token, TextSendMessage(text="票券缺少票號，請重新拍攝或輸入"))
                    return "OK"
                if check_if_ticket_exists(ticket_id, user_ticket_path):
                    reply_msg = get_train_ticket_flex_msg(ticket, segments)
                    chinese_reply_msg = get_train_ticket_flex_msg(tw_ticket, tw_segments)
                    await line_bot_api.reply_message(
                        event.reply_token,
                        [TextSendMessage(text="這張票券已經存在資料庫中。"), reply_msg, chinese_reply_msg],
                    )
                    return "OK"
                add_ticket(tw_ticket, tw_segments, user_ticket_path, user_segment_path)
                reply_msg = get_train_ticket_flex_msg(ticket, segments)
                chinese_reply_msg = get_train_ticket_flex_msg(tw_ticket, tw_segments)
                await line_bot_api.reply_message(event.reply_token, [reply_msg, chinese_reply_msg])
                return "OK"
            logger.warning("無法辨識為收據或票券")
            await line_bot_api.reply_message(event.reply_token, TextSendMessage(text="無法辨識為收據或票券，請重新拍攝"))
            return "OK"
        else:
            continue
    return "OK"


@app.get("/")
def home():
    return "Hello World"


if __name__ == "__main__":
    pass
