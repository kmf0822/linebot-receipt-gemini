import json
from datetime import datetime
from typing import Any, Dict, List

import gspread
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials

from src.logger import logger

_SCOPES = (
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
)
_RECEIPT_COLUMNS = [
    "UserID",
    "ReceiptID",
    "PurchaseStore",
    "PurchaseDate",
    "PurchaseAddress",
    "TotalAmount",
    "ItemsJSON",
    "CreatedAt",
]
_TICKET_COLUMNS = [
    "UserID",
    "TicketID",
    "CarrierName",
    "RouteNumber",
    "TicketType",
    "DepartureStation",
    "ArrivalStation",
    "DepartureTime",
    "ArrivalTime",
    "PassengerName",
    "TotalAmount",
    "SegmentsJSON",
    "CreatedAt",
]


class SheetsStorage:
    """Simple persistence layer for receipts and tickets stored in Google Sheets."""

    RECEIPTS_SHEET = "Receipts"
    TICKETS_SHEET = "Tickets"

    def __init__(self, spreadsheet_id: str, credentials_json: str):
        if not spreadsheet_id:
            raise ValueError("spreadsheet_id is required")
        self.spreadsheet_id = spreadsheet_id
        self.client = gspread.authorize(self._build_credentials(credentials_json))
        try:
            self.spreadsheet = self.client.open_by_key(spreadsheet_id)
        except SpreadsheetNotFound as exc:
            logger.error(f"Spreadsheet {spreadsheet_id} not found: {exc}")
            raise
        self.receipts_ws = self._ensure_worksheet(self.RECEIPTS_SHEET, _RECEIPT_COLUMNS)
        self.tickets_ws = self._ensure_worksheet(self.TICKETS_SHEET, _TICKET_COLUMNS)

    @staticmethod
    def _build_credentials(credentials_json: str) -> Credentials:
        if not credentials_json:
            raise ValueError("credentials_json is required")
        try:
            info = json.loads(credentials_json)
        except json.JSONDecodeError:
            with open(credentials_json, "r", encoding="utf-8") as handle:
                info = json.load(handle)
        return Credentials.from_service_account_info(info, scopes=_SCOPES)

    def _ensure_worksheet(self, title: str, columns: List[str]):
        try:
            worksheet = self.spreadsheet.worksheet(title)
        except WorksheetNotFound:
            worksheet = self.spreadsheet.add_worksheet(title=title, rows="500", cols=str(len(columns) + 2))
        self._ensure_header_row(worksheet, columns)
        return worksheet

    @staticmethod
    def _ensure_header_row(worksheet, columns: List[str]):
        current_header = worksheet.row_values(1)
        if current_header == columns:
            return
        end_cell = rowcol_to_a1(1, len(columns))
        worksheet.update(f"A1:{end_cell}", [columns])

    @staticmethod
    def _now() -> str:
        return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    @staticmethod
    def _dumps(data: Any) -> str:
        return json.dumps(data or [], ensure_ascii=False)

    def store_receipt(self, user_id: str, receipt_data: Dict[str, Any], items: List[Dict[str, Any]]):
        if not receipt_data:
            raise ValueError("receipt_data is required")
        row = [
            user_id,
            receipt_data.get("ReceiptID", ""),
            receipt_data.get("PurchaseStore", ""),
            receipt_data.get("PurchaseDate", ""),
            receipt_data.get("PurchaseAddress", ""),
            receipt_data.get("TotalAmount", ""),
            self._dumps(items),
            self._now(),
        ]
        self.receipts_ws.append_row(row, value_input_option="RAW")

    def store_ticket(self, user_id: str, ticket_data: Dict[str, Any], segments: List[Dict[str, Any]]):
        if not ticket_data:
            raise ValueError("ticket_data is required")
        row = [
            user_id,
            ticket_data.get("TicketID", ""),
            ticket_data.get("CarrierName", ""),
            ticket_data.get("RouteNumber", ""),
            ticket_data.get("TicketType", ""),
            ticket_data.get("DepartureStation", ""),
            ticket_data.get("ArrivalStation", ""),
            ticket_data.get("DepartureTime", ""),
            ticket_data.get("ArrivalTime", ""),
            ticket_data.get("PassengerName", ""),
            ticket_data.get("TotalAmount", ""),
            self._dumps(segments),
            self._now(),
        ]
        self.tickets_ws.append_row(row, value_input_option="RAW")

    def receipt_exists(self, user_id: str, receipt_id: str) -> bool:
        if not receipt_id:
            return False
        return self._row_exists(self.receipts_ws, user_id, "ReceiptID", receipt_id)

    def ticket_exists(self, user_id: str, ticket_id: str) -> bool:
        if not ticket_id:
            return False
        return self._row_exists(self.tickets_ws, user_id, "TicketID", ticket_id)

    @staticmethod
    def _row_matches(row: Dict[str, Any], user_id: str, key: str, value: str) -> bool:
        return str(row.get("UserID", "")) == user_id and str(row.get(key, "")) == value

    def _row_exists(self, worksheet, user_id: str, key: str, value: str) -> bool:
        for row in worksheet.get_all_records():
            if self._row_matches(row, user_id, key, value):
                return True
        return False

    def get_user_snapshot(self, user_id: str) -> str:
        receipts = self._deserialize_rows(self.receipts_ws.get_all_records(), user_id, "ItemsJSON", "Items")
        tickets = self._deserialize_rows(self.tickets_ws.get_all_records(), user_id, "SegmentsJSON", "Segments")
        payload = {"receipts": receipts, "tickets": tickets}
        return json.dumps(payload, ensure_ascii=False)

    @staticmethod
    def _deserialize_rows(rows: List[Dict[str, Any]], user_id: str, serialized_key: str, target_key: str) -> List[Dict[str, Any]]:
        filtered: List[Dict[str, Any]] = []
        for row in rows:
            if str(row.get("UserID", "")) != user_id:
                continue
            data = {k: v for k, v in row.items() if k not in {"UserID", serialized_key, "CreatedAt"}}
            serialized = row.get(serialized_key)
            if serialized:
                try:
                    data[target_key] = json.loads(serialized)
                except json.JSONDecodeError:
                    data[target_key] = serialized
            else:
                data[target_key] = []
            filtered.append(data)
        return filtered

    def clear_user_data(self, user_id: str):
        self._delete_rows_by_user(self.receipts_ws, user_id)
        self._delete_rows_by_user(self.tickets_ws, user_id)

    @staticmethod
    def _delete_rows_by_user(worksheet, user_id: str):
        values = worksheet.get_all_values()
        if len(values) <= 1:
            return
        rows_to_delete: List[int] = []
        for idx, row in enumerate(values[1:], start=2):
            if row and row[0] == user_id:
                rows_to_delete.append(idx)
        for row_idx in reversed(rows_to_delete):
            worksheet.delete_rows(row_idx)
