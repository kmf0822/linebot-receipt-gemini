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

# Environment variables
channel_secret = os.getenv("ChannelSecret")
channel_access_token = os.getenv("ChannelAccessToken")
openai_api_key = os.getenv("AZURE_OPENAI_API_KEY")
openai_model_engine = os.getenv("AZURE_OPENAI_MODEL_ENGINE")
firebase_url = os.getenv("FIREBASE_URL")

image_prompt = """\
This is a receipt, and you are a secretary.
Please organize the details from the receipt into JSON format for me.
I only need the JSON representation of the receipt data. Eventually,
I will need to input it into a database with the following structure:

 Receipt(ReceiptID, PurchaseStore, PurchaseDate, PurchaseAddress, TotalAmount) and
 Items(ItemID, ReceiptID, ItemName, ItemPrice).

Data format as follow:
- ReceiptID, using PurchaseDate, but Represent the year, month, day, hour, and minute without any separators.
- ItemID, using ReceiptID and sequel number in that receipt.
Otherwise, if any information is unclear, fill in with 'N/A'.
"""

json_translate_from_nonchinese_prompt = """\
This is a JSON representation of a receipt.
Please translate the non-Chinese characters into Chinese for me.
Using format as follow:
    non-Chinese(Chinese)
All the Chinese will use in zh_tw.
Please response with the translated JSON.
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
        print(f"Specify {var} as environment variable.")
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
    print(f"Azure OpenAI error: {err}")
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
    print(f"Azure OpenAI error: {err}")
    return ""


# ================= Firebase =================
def add_receipt(receipt_data: dict, items: list, user_receipt_path: str, user_item_path: str):
    try:
        receipt_id = receipt_data.get("ReceiptID")
        db.reference(user_receipt_path).child(receipt_id).set(receipt_data)
        for item in items:
            item_id = item.get("ItemID")
            db.reference(user_item_path).child(item_id).set(item)
        print(f"Add ReceiptID: {receipt_id} completed.")
    except Exception as e:
        print(f"Error in add_receipt: {e}")


def check_if_receipt_exists(receipt_id: str, user_receipt_path: str) -> bool:
    try:
        receipt = db.reference(user_receipt_path).child(receipt_id).get()
        return receipt is not None
    except Exception as e:
        print(f"Error in check_if_receipt_exists: {e}")
        return False


# ================= Data Processing =================
def parse_receipt_json(receipt_json_str: str):
    try:
        lines = receipt_json_str.strip().split('\n')
        json_str = '\n'.join(lines[1:-1])
        receipt_data = json.loads(json_str)
        return receipt_data
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return None


def extract_receipt_data(receipt_json_obj: dict):
    receipt_obj = None
    items = []
    if receipt_json_obj:
        receipt_obj = receipt_json_obj.get("Receipt")
        if receipt_obj and isinstance(receipt_obj, list):
            receipt_obj = receipt_obj[0]
        items = receipt_json_obj.get("Items", [])
    return items, receipt_obj


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
                {"type": "text", "text": "RECEIPT", "weight": "bold", "color": "#1DB446", "size": "sm"},
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
                        {"type": "text", "text": "RECEIPT ID", "size": "xs", "color": "#aaaaaa", "flex": 0},
                        {"type": "text", "text": f"{receipt_data.get('ReceiptID')}", "color": "#aaaaaa", "size": "xs", "align": "end"},
                    ],
                },
            ],
        },
        "styles": {"footer": {"separator": True}},
    }
    return FlexSendMessage(alt_text="Receipt Data", contents=flex_msg)


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
            await line_bot_api.reply_message(event.reply_token, reply_msg)
        elif event.message.type == "image":
            message_content = await line_bot_api.get_message_content(event.message.id)
            image_content = b""
            async for s in message_content.iter_content():
                image_content += s
            img = PIL.Image.open(BytesIO(image_content))
            result_text = generate_json_from_receipt_image(img, image_prompt)
            print(f"Before Translate Result: {result_text}")
            tw_result_text = generate_aoai_text_complete(
                result_text + "\n --- " + json_translate_from_nonchinese_prompt
            )
            print(f"After Translate Result: {tw_result_text}")
            items, receipt = extract_receipt_data(parse_receipt_json(result_text))
            tw_items, tw_receipt = extract_receipt_data(parse_receipt_json(tw_result_text))
            if check_if_receipt_exists(receipt.get("ReceiptID"), user_receipt_path):
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
        else:
            continue
    return "OK"


@app.get("/")
def home():
    return "Hello World"


if __name__ == "__main__":
    pass
