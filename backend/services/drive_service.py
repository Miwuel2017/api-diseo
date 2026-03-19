from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/drive']

SERVICE_ACCOUNT_FILE = 'config/credentials.json'


def get_drive_service():

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )

    service = build('drive', 'v3', credentials=credentials)

    return service

def list_files_in_folder(folder_id):

    service = get_drive_service()

    query = f"'{folder_id}' in parents"

    results = service.files().list(
        q=query,
        fields="files(id, name, mimeType)"
    ).execute()

    return results.get('files', [])

from googleapiclient.http import MediaIoBaseDownload
import io


def download_file(file_id, mime_type):

    service = get_drive_service()

    file_stream = io.BytesIO()

    # Si es Google Sheet
    if mime_type == "application/vnd.google-apps.spreadsheet":

        request = service.files().export_media(
            fileId=file_id,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    else:
        request = service.files().get_media(fileId=file_id)

    downloader = MediaIoBaseDownload(file_stream, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    file_stream.seek(0)

    return file_stream