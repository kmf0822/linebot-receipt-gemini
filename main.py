import base64
import json
import os
import sys
import uuid
from io import BytesIO

import PIL.Image
import aiohttp
from fastapi import FastAPI, HTTPException, Request
from linebot import AsyncLineBotApi, WebhookParser
from linebot.aiohttp_async_http_client import AiohttpAsyncHttpClient
from linebot.exceptions import InvalidSignatureError
from linebot.models import FlexSendMessage, MessageEvent, TextSendMessage
from linebot.v3.messaging import (
    ApiClient,
    Configuration,
    MessagingApi,
    ShowLoadingAnimationRequest,
)

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
Translate every non-Chinese value into zh_tw, using the format Chinese(non-Chinese).
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
session = aiohttp.ClientSession()
async_http_client = AiohttpAsyncHttpClient(session)
line_bot_api = AsyncLineBotApi(channel_access_token, async_http_client)
parser = WebhookParser(channel_secret)

# Initialize v3 MessagingApi for show_loading_animation
configuration = Configuration(access_token=channel_access_token)
api_client = ApiClient(configuration)
line_bot_api_v3 = MessagingApi(api_client)

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
            reply_msg = TextSendMessage(text="No message to reply with")
            if text == "!清空":
                reply_msg = TextSendMessage(text="對話歷史紀錄已經清空！")
                sheets_storage.clear_user_data(user_id)
            else:
                prompt_msg = (
                    f"Here is my entire receipt list during my travel: {all_receipts}; "
                    f"please answer my question based on this information. {text}. Reply in zh_tw."
                )
                response_text = generate_aoai_text_complete(prompt_msg)
                reply_msg = TextSendMessage(text=response_text)
            logger.info(f'{user_id}: [{openai_model_engine}]' + text)
            await line_bot_api.reply_message(event.reply_token, reply_msg)
        elif event.message.type == "image":
            message_content = await line_bot_api.get_message_content(event.message.id)
            image_content = b""
            async for s in message_content.iter_content():
                image_content += s
            img = PIL.Image.open(BytesIO(image_content))

            # Save image to local file for uploading to Google Drive
            input_image_path = f'{str(uuid.uuid4())}.jpg'
            img.save(input_image_path, format='JPEG')

            logger.info(f'{user_id}: [{openai_model_engine}]{event.message.type} received')
            result_text = generate_json_from_receipt_image(img, image_prompt)
            if not result_text:
                logger.warning("模型沒有回傳任何資料")
                await line_bot_api.reply_message(event.reply_token, TextSendMessage(text="未取得辨識結果，請稍後再試"))
                # Clean up temp image file
                if os.path.exists(input_image_path):
                    os.remove(input_image_path)
                return "OK"
            # logger.info(f'{user_id}: [{openai_model_engine}] Before Translate Result: {result_text}')
            tw_result_text = generate_aoai_text_complete(
                result_text + "\n --- " + json_translate_from_nonchinese_prompt
            )
            logger.info(f'{user_id}: [{openai_model_engine}]{tw_result_text} After Translate Result: {tw_result_text}')
            parsed_result = parse_receipt_json(result_text)
            if parsed_result is None:
                await line_bot_api.reply_message(event.reply_token, TextSendMessage(text="資料解析失敗，請確認影像內容"))
                # Clean up temp image file
                if os.path.exists(input_image_path):
                    os.remove(input_image_path)
                return "OK"
            parsed_tw_result = parse_receipt_json(tw_result_text)
            if parsed_tw_result is None:
                await line_bot_api.reply_message(event.reply_token, TextSendMessage(text="翻譯後資料解析失敗，請稍後再試"))
                # Clean up temp image file
                if os.path.exists(input_image_path):
                    os.remove(input_image_path)
                return "OK"
            items, receipt = extract_receipt_data(parsed_result)
            segments, ticket = extract_ticket_data(parsed_result)
            tw_items, tw_receipt = extract_receipt_data(parsed_tw_result)
            tw_segments, tw_ticket = extract_ticket_data(parsed_tw_result)
            if receipt:
                if tw_receipt is None:
                    logger.warning("翻譯後收據資料解析失敗")
                    await line_bot_api.reply_message(event.reply_token, TextSendMessage(text="收據資料解析失敗，請檢查圖片或資料格式"))
                    # Clean up temp image file
                    if os.path.exists(input_image_path):
                        os.remove(input_image_path)
                    return "OK"
                receipt_id = receipt.get("ReceiptID")
                reply_messages = []
                if check_if_receipt_exists(user_id, receipt_id):
                    reply_messages.append(TextSendMessage(text="這個收據已經存在資料庫中。"))
                else:
                    add_receipt(user_id, tw_receipt, tw_items, image_path=input_image_path)
                reply_msg = get_receipt_flex_msg(receipt, items)
                chinese_reply_msg = get_receipt_flex_msg(tw_receipt, tw_items)
                # reply_messages.append(reply_msg)
                reply_messages.append(chinese_reply_msg)
                logger.info(f'{user_id}: [{openai_model_engine}]{receipt_id} Receipt processed and reply sent')
                await line_bot_api.reply_message(event.reply_token, reply_messages)
                # Clean up temp image file
                if os.path.exists(input_image_path):
                    os.remove(input_image_path)
                return "OK"
            if ticket:
                if tw_ticket is None:
                    logger.warning("翻譯後票券資料解析失敗")
                    await line_bot_api.reply_message(event.reply_token, TextSendMessage(text="票券資料解析失敗，請檢查圖片或資料格式"))
                    # Clean up temp image file
                    if os.path.exists(input_image_path):
                        os.remove(input_image_path)
                    return "OK"
                ticket_id = ticket.get("TicketID")
                if not ticket_id:
                    logger.warning("票券缺少 TicketID")
                    await line_bot_api.reply_message(event.reply_token, TextSendMessage(text="票券缺少票號，請重新拍攝或輸入"))
                    # Clean up temp image file
                    if os.path.exists(input_image_path):
                        os.remove(input_image_path)
                    return "OK"
                reply_messages = []
                if check_if_ticket_exists(user_id, ticket_id):
                    reply_messages.append(TextSendMessage(text="這張票券已經存在資料庫中。"))
                else:
                    add_ticket(user_id, tw_ticket, tw_segments, image_path=input_image_path)
                reply_msg = get_train_ticket_flex_msg(ticket, segments)
                chinese_reply_msg = get_train_ticket_flex_msg(tw_ticket, tw_segments)
                # reply_messages.append(reply_msg)
                reply_messages.append(chinese_reply_msg)
                logger.info(f'{user_id}: [{openai_model_engine}]{ticket_id} Ticket processed and reply sent')
                await line_bot_api.reply_message(event.reply_token, reply_messages)
                # Clean up temp image file
                if os.path.exists(input_image_path):
                    os.remove(input_image_path)
                return "OK"
            # Clean up temp image file if neither receipt nor ticket
            if os.path.exists(input_image_path):
                os.remove(input_image_path)
    return None


@app.get("/")
def home():
    return "Hello World"


if __name__ == "__main__":
    pass
