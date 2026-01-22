import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import gspread
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from src.logger import logger

_SCOPES = (
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
)

# Root folder ID for storing images in personal Google Drive
# To use this:
# 1. Create a folder in your personal Google Drive
# 2. Share that folder with the service account and give it "Editor" access
# 3. Copy the folder ID from the URL (the long string after /folders/)
# 4. Set this environment variable: GOOGLE_DRIVE_ROOT_FOLDER_ID=<your_folder_id>
GOOGLE_DRIVE_ROOT_FOLDER_ID = os.getenv('GOOGLE_DRIVE_ROOT_FOLDER_ID')
if GOOGLE_DRIVE_ROOT_FOLDER_ID:
    logger.debug(f"Using personal Google Drive root folder ID: {GOOGLE_DRIVE_ROOT_FOLDER_ID}")
_RECEIPT_COLUMNS = [
    "UserID",
    "ReceiptID",
    "PurchaseStore",
    "PurchaseDate",
    "PurchaseAddress",
    "TotalAmount",
    "ItemsJSON",
    "ImageURL",
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
    "ImageURL",
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
        self._credentials = self._build_credentials(credentials_json)
        self.client = gspread.authorize(self._credentials)
        self._drive_service = None
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

    def _get_drive_service(self):
        """Initialize and return Google Drive API service."""
        if self._drive_service is None:
            self._drive_service = build('drive', 'v3', credentials=self._credentials)
        return self._drive_service

    def _find_or_create_folder(self, folder_name: str, parent_id: Optional[str] = None) -> Optional[str]:
        """
        Find or create a folder in Google Drive.

        Args:
            folder_name: Name of the folder to find or create.
            parent_id: Optional parent folder ID.

        Returns:
            Folder ID, or None if failed.
        """
        try:
            drive_service = self._get_drive_service()

            # Search for existing folder
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_id:
                query += f" and '{parent_id}' in parents"

            results = drive_service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            files = results.get('files', [])

            if files:
                logger.debug(f"Found existing folder '{folder_name}' with ID: {files[0].get('id')}")
                return files[0].get('id')

            # Create folder if not exists
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            if parent_id:
                file_metadata['parents'] = [parent_id]

            folder = drive_service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
            logger.debug(f"Created folder '{folder_name}' with ID: {folder.get('id')}")
            return folder.get('id')
        except Exception as e:
            logger.error(f"Error finding or creating folder '{folder_name}': {e}")
            return None

    def _get_image_folder_id(self, user_id: str, image_type: str) -> Optional[str]:
        """
        Get or create the folder hierarchy for storing images.
        Structure: {root_folder} / ReceiptBot / {user_id} / {image_type}

        If GOOGLE_DRIVE_ROOT_FOLDER_ID is set, the root_folder will be the shared folder
        in personal Google Drive. Otherwise, files will be stored in the service account's Drive.

        Args:
            user_id: LINE user ID.
            image_type: Type of image ('receipts' or 'tickets').

        Returns:
            Folder ID for storing the image, or None if failed.
        """
        # Use personal Google Drive folder if configured, otherwise use service account's Drive
        parent_folder_id = GOOGLE_DRIVE_ROOT_FOLDER_ID

        # Create root folder: ReceiptBot (inside personal folder if configured)
        root_folder_id = self._find_or_create_folder('ReceiptBot', parent_folder_id)
        if not root_folder_id:
            return None

        # Create user folder: ReceiptBot / {user_id}
        user_folder_id = self._find_or_create_folder(user_id, root_folder_id)
        if not user_folder_id:
            return None

        # Create image type folder: ReceiptBot / {user_id} / {image_type}
        image_type_folder_id = self._find_or_create_folder(image_type, user_folder_id)
        return image_type_folder_id

    def upload_image_to_drive(self, file_path: str, user_id: str, image_type: str) -> Optional[str]:
        """
        Upload image to Google Drive and return the file ID.

        Args:
            file_path: Path to the image file to upload.
            user_id: LINE user ID for folder organization.
            image_type: Type of image ('receipts' or 'tickets').

        Returns:
            File ID of the uploaded image, or None if upload fails.
        """
        try:
            logger.debug(f"[{image_type=}] {file_path=}")
            drive_service = self._get_drive_service()

            # Get or create the target folder
            folder_id = self._get_image_folder_id(user_id, image_type)
            file_metadata = {'name': os.path.basename(file_path)}
            if folder_id:
                file_metadata['parents'] = [folder_id]
            logger.debug(f"{file_metadata=}")

            media = MediaFileUpload(file_path, mimetype='image/jpeg')
            file = drive_service.files().create(
                body=file_metadata, media_body=media, fields='id'
            ).execute()
            return file.get('id')
        except Exception as e:
            logger.error(f"Error uploading image to Google Drive: {e}")
            return None

    def get_shareable_link(self, file_id: str) -> Optional[str]:
        """
        Get shareable link for a Google Drive file.

        Args:
            file_id: The Google Drive file ID.

        Returns:
            Shareable URL of the file, or None if failed.
        """
        try:
            drive_service = self._get_drive_service()
            drive_service.permissions().create(
                fileId=file_id,
                body={'type': 'anyone', 'role': 'reader'}
            ).execute()
            file = drive_service.files().get(fileId=file_id, fields='webViewLink').execute()
            return file.get('webViewLink')
        except Exception as e:
            logger.error(f"Error getting shareable link: {e}")
            return None

    def upload_and_get_image_url(self, file_path: str, user_id: str, image_type: str) -> Optional[str]:
        """
        Upload image to Google Drive and return the shareable URL.

        Args:
            file_path: Path to the image file to upload.
            user_id: LINE user ID for folder organization.
            image_type: Type of image ('receipts' or 'tickets').

        Returns:
            Shareable URL of the uploaded image, or None if failed.
        """
        if not file_path or not os.path.exists(file_path):
            return None
        logger.debug(f"[{image_type=}] {file_path=}")
        file_id = self.upload_image_to_drive(file_path, user_id, image_type)
        if not file_id:
            return None
        return self.get_shareable_link(file_id)

    @staticmethod
    def get_image_formula(image_url: str) -> str:
        """
        Generate Google Sheets IMAGE formula for displaying image in cell.

        Args:
            image_url: URL of the image.

        Returns:
            IMAGE formula string for Google Sheets.
        """
        if not image_url:
            return ""
        return f'=IMAGE("{image_url}")'

    def store_receipt(self, user_id: str, receipt_data: Dict[str, Any], items: List[Dict[str, Any]], image_path: Optional[str] = None):
        """
        Store receipt data to Google Sheets.

        Args:
            user_id: LINE user ID.
            receipt_data: Receipt information dictionary.
            items: List of receipt items.
            image_path: Optional path to the receipt image file.
        """
        if not receipt_data:
            raise ValueError("receipt_data is required")

        # Upload image and get shareable URL if image_path is provided
        image_formula = ""
        if image_path:
            image_url = self.upload_and_get_image_url(image_path, user_id, 'receipts')
            if image_url:
                image_formula = self.get_image_formula(image_url)

        row = [
            user_id,
            receipt_data.get("ReceiptID", ""),
            receipt_data.get("PurchaseStore", ""),
            receipt_data.get("PurchaseDate", ""),
            receipt_data.get("PurchaseAddress", ""),
            receipt_data.get("TotalAmount", ""),
            self._dumps(items),
            image_formula,
            self._now(),
        ]
        self.receipts_ws.append_row(row, value_input_option="USER_ENTERED")

    def store_ticket(self, user_id: str, ticket_data: Dict[str, Any], segments: List[Dict[str, Any]], image_path: Optional[str] = None):
        """
        Store ticket data to Google Sheets.

        Args:
            user_id: LINE user ID.
            ticket_data: Ticket information dictionary.
            segments: List of ticket segments.
            image_path: Optional path to the ticket image file.
        """
        if not ticket_data:
            raise ValueError("ticket_data is required")

        # Upload image and get shareable URL if image_path is provided
        image_formula = ""
        if image_path:
            image_url = self.upload_and_get_image_url(image_path, user_id, 'tickets')
            if image_url:
                image_formula = self.get_image_formula(image_url)

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
            image_formula,
            self._now(),
        ]
        self.tickets_ws.append_row(row, value_input_option="USER_ENTERED")

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
