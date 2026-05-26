import os
import io
import time
import zipfile
import aiohttp
from telethon import TelegramClient, events

# ==================== CONFIGURATION ====================
# These variables are automatically pulled from your GitHub Secrets
API_ID = int(os.environ.get("API_ID", 1234567))
API_HASH = os.environ.get("API_HASH", "your_api_hash")
SESSION_NAME = "bunny_pipeline_bot"

BUNNY_LIBRARY_ID = os.environ.get("BUNNY_LIBRARY_ID", "12345")
BUNNY_API_KEY = os.environ.get("BUNNY_API_KEY", "your-api-key")
# =======================================================

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

def generate_progress_bar(percentage, length=15):
    """Generates a visual scannable progress bar."""
    filled_length = int(length * percentage // 100)
    return '█' * filled_length + '░' * (length - filled_length)


async def get_bunny_video_id(title, library_id, api_key):
    """Creates a placeholder slot on Bunny Stream and returns the Video ID."""
    url = f"https://video.bunnycdn.com/library/{library_id}/videos"
    headers = {
        "AccessKey": api_key,
        "Content-Type": "application/json",
        "accept": "application/json"
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json={"title": title}) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("guid")
            return None


async def piped_upload_to_bunny(client, message, video_id, total_size, library_id, api_key, status_msg):
    """Streams data straight from Telegram CDN to Bunny CDN via memory pipe."""
    url = f"https://video.bunnycdn.com/library/{library_id}/videos/{video_id}"
    headers = {
        "AccessKey": api_key,
        "Content-Type": "application/octet-stream",
        "Content-Length": str(total_size)
    }
    
    uploaded_bytes = 0
    last_update_time = 0
    start_time = time.time()
    
    async def data_pipe_generator():
        nonlocal uploaded_bytes, last_update_time
        # Pulls chunks directly into memory buffer and yields them instantly to Bunny
        async for chunk in client.iter_download(message.media, chunk_size=1024 * 1024):
            yield chunk
            uploaded_bytes += len(chunk)
            
            now = time.time()
            # Throttled UI edits to safe 3.5-second windows to protect from Telegram FloodWaits
            if now - last_update_time > 3.5 or uploaded_bytes == total_size:
                pct = (uploaded_bytes / total_size) * 100
                bar = generate_progress_bar(pct)
                elapsed = now - start_time
                speed = (uploaded_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                
                status_text = (
                    f"⚡ **Feature 2: Piped Streaming Active**\n"
                    f"`[{bar}]` {pct:.2f}%\n\n"
                    f"⚙️ **Progress:** {uploaded_bytes / (1024*1024):.2f} MB / {total_size / (1024*1024):.2f} MB\n"
                    f"🚀 **Speed:** {speed:.2f} MB/s"
                )
                try:
                    await client.edit_message(message.chat_id, status_msg.id, status_text)
                except Exception:
                    pass
                last_update_time = now

    async with aiohttp.ClientSession() as session:
        async with session.put(url, headers=headers, data=data_pipe_generator()) as resp:
            return resp.status == 200


def extract_and_map_zip(file_bytes, indent_level=0, current_index=[1]):
    """Recursively extracts zip archives completely in RAM and formats a structural list layout."""
    output = ""
    indent = "    " * indent_level  # Non-breaking space padding for clear Telegram folder hierarchy indentation
    
    try:
        with zipfile.ZipFile(file_bytes) as z:
            for member in z.infolist():
                if member.is_dir():
                    continue
                
                filename = member.filename
                
                # If a nested zip structure is found inside the parent archive
                if filename.lower().endswith('.zip'):
                    output += f"{indent}{current_index[0]}. 📁 `{filename}` **(Nested Zip Found -> Extracting... )**\n"
                    current_index[0] += 1
                    
                    # Open nested archive file pointer directly into memory stream for recursion
                    with z.open(member) as nested_file:
                        nested_bytes = io.BytesIO(nested_file.read())
                        output += extract_and_map_zip(nested_bytes, indent_level + 1, current_index)
                else:
                    output += f"{indent}{current_index[0]}. 📄 `{filename}`\n"
                    current_index[0] += 1
                    
    except zipfile.BadZipFile:
        output += f"{indent}⚠️ _[Error: Corrupted or encrypted inner zip file encountered]_\n"
        
    return output


@client.on(events.NewMessage(outgoing=True))
async def handle_userbot_media(event):
    message = event.message
    if not message.file:
        return

    filename = message.file.name or "unnamed_file"
    ext = os.path.splitext(filename)[1].lower() if '.' in filename else ""
    total_size = message.file.size

    # --- FEATURE 1: NESTED ZIP EXTRACTION & MAP NAME GENERATION ---
    if ext == '.zip':
        status_msg = await event.reply("📥 **Feature 1: Downloading Zip to RAM for Deep Extraction...**")
        
        try:
            # Streams file down directly to RAM buffer object
            buffer = io.BytesIO()
            async for chunk in client.iter_download(message.media):
                buffer.write(chunk)
            buffer.seek(0)
            
            await client.edit_message(event.chat_id, status_msg.id, "⚙️ **Extracting layers and compiling deep file tree map...**")
            
            file_tree = extract_and_map_zip(buffer)
            
            final_output = f"📦 **Deep ZIP Extraction Map for:** `{filename}`\n\n"
            if file_tree.strip():
                final_output += file_tree
            else:
                final_output += "📂 _(The archive appears empty or contains only folders)_"
            
            # Enforce message text constraints safely
            if len(final_output) > 4096:
                final_output = final_output[:4000] + "\n\n⚠️ *[Structure truncated due to Telegram message limits]*"
                
            await client.edit_message(event.chat_id, status_msg.id, final_output)
            
        except Exception as e:
            await client.edit_message(event.chat_id, status_msg.id, f"❌ **Extraction failed:** `{str(e)}`")

    # --- FEATURE 2: VIDEO PIPE STREAMING TO BUNNY ---
    elif ext in ['.mp4', '.mkv', '.ts', '.mov', '.avi', '.webm', '.flv']:
        status_msg = await event.reply(f"🎬 **Feature 2: Initializing Pipe for:** `{filename}`")
        
        video_id = await get_bunny_video_id(filename, BUNNY_LIBRARY_ID, BUNNY_API_KEY)
        if not video_id:
            await client.edit_message(event.chat_id, status_msg.id, "❌ **Failed to set up container on Bunny Stream.**")
            return
            
        success = await piped_upload_to_bunny(client, message, video_id, total_size, BUNNY_LIBRARY_ID, BUNNY_API_KEY, status_msg)
        
        if success:
            await client.edit_message(
                event.chat_id, 
                status_msg.id, 
                f"✅ **Piped Upload Complete!**\n\n"
                f"📛 **File Name:** `{filename}`\n"
                f"🆔 **Bunny Video ID:** `{video_id}`"
            )
        else:
            await client.edit_message(event.chat_id, status_msg.id, f"❌ **Pipeline failed mid-stream for:** `{filename}`")

print("⚡ Userbot loaded with 2 main features. Monitoring events...")
client.start()
client.run_until_disconnected()
