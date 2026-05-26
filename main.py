import os
import io
import time
import zipfile
import aiohttp
import asyncio
import gc  # Garbage collection to explicitly clean RAM
from telethon import TelegramClient, events, functions, utils  # Added utils for safe conversion

# ==================== CONFIGURATION ====================
API_ID = int(os.environ.get("API_ID", 1234567))
API_HASH = os.environ.get("API_HASH", "your_api_hash")
SESSION_NAME = "bunny_pipeline_bot"

BUNNY_LIBRARY_ID = os.environ.get("BUNNY_LIBRARY_ID", "12345")
BUNNY_API_KEY = os.environ.get("BUNNY_API_KEY", "your-api-key")
# =======================================================

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

def generate_progress_bar(percentage, length=15):
    filled_length = int(length * percentage // 100)
    return '█' * filled_length + '░' * (length - filled_length)

def format_time(seconds):
    if seconds is None or seconds < 0:
        return "Calculating..."
    seconds = int(seconds)
    if seconds < 3600:
        return f"{seconds // 60:02d}m:{seconds % 60:02d}s"
    return f"{seconds // 3600:02d}h:{(seconds % 3600) // 60:02d}m:{seconds % 60:02d}s"

async def get_bunny_video_id(title, library_id, api_key):
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
        async for chunk in client.iter_download(message.media, chunk_size=1024 * 1024):
            yield chunk
            uploaded_bytes += len(chunk)
            
            now = time.time()
            if now - last_update_time > 4.5 or uploaded_bytes == total_size:
                pct = (uploaded_bytes / total_size) * 100
                bar = generate_progress_bar(pct)
                elapsed = now - start_time
                speed = uploaded_bytes / elapsed if elapsed > 0 else 0
                
                remaining_bytes = total_size - uploaded_bytes
                eta = remaining_bytes / speed if speed > 0 else 0
                
                status_text = (
                    f"⚡ **Feature 2: Piped Streaming Active**\n"
                    f"`[{bar}]` {pct:.2f}%\n\n"
                    f"⚙️ **Progress:** {uploaded_bytes / (1024*1024):.2f} MB / {total_size / (1024*1024):.2f} MB\n"
                    f"🚀 **Speed:** {(speed / (1024*1024)):.2f} MB/s\n"
                    f"⏱️ **Elapsed Time:** {format_time(elapsed)}\n"
                    f"⏳ **Estimated Remaining (ETA):** {format_time(eta)}"
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
    output = ""
    indent = "    " * indent_level
    
    try:
        with zipfile.ZipFile(file_bytes) as z:
            for member in z.infolist():
                if member.is_dir():
                    continue
                filename = member.filename
                if filename.lower().endswith('.zip'):
                    output += f"{indent}{current_index[0]}. 📁 `{filename}` **(Nested Zip Found -> Extracting... )**\n"
                    current_index[0] += 1
                    with z.open(member) as nested_file:
                        nested_bytes = io.BytesIO(nested_file.read())
                        output += extract_and_map_zip(nested_bytes, indent_level + 1, current_index)
                else:
                    output += f"{indent}{current_index[0]}. 📄 `{filename}`\n"
                    current_index[0] += 1
    except zipfile.BadZipFile:
        output += f"{indent}⚠️ _[Error: Corrupted or encrypted inner zip file encountered]_\n"
    return output

# --- FIXED PARALLEL ENGINE: SAFELY MAPS LOCATION VIA UTILS ---
async def fast_parallel_download(client, message, status_msg, event):
    total_size = message.file.size
    chunk_size = 1024 * 1024  # 1MB Chunks
    concurrency = 8           # Parallel streams
    
    # FIX: Uses Telethon's built-in safe conversion utility directly on media object
    file_location, file_type = utils.get_input_location(message.media)
    
    downloaded_buffer = bytearray(total_size)
    offsets = list(range(0, total_size, chunk_size))
    
    downloaded_bytes = 0
    last_update_time = time.time()
    start_time = time.time()
    
    queue = asyncio.Queue()
    for offset in offsets:
        queue.put_nowait(offset)
        
    async def worker():
        nonlocal downloaded_bytes, last_update_time
        while not queue.empty():
            try:
                offset = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
                
            result = await client(functions.upload.GetFileRequest(
                location=file_location,
                offset=offset,
                limit=chunk_size
            ))
            
            downloaded_buffer[offset:offset+len(result.bytes)] = result.bytes
            downloaded_bytes += len(result.bytes)
            queue.task_done()
            
            now = time.time()
            if now - last_update_time > 4.5 or downloaded_bytes == total_size:
                pct = (downloaded_bytes / total_size) * 100
                bar = generate_progress_bar(pct)
                elapsed = now - start_time
                speed = downloaded_bytes / elapsed if elapsed > 0 else 0
                
                remaining_bytes = total_size - downloaded_bytes
                eta = remaining_bytes / speed if speed > 0 else 0
                
                status_text = (
                    f"🚀 **Feature 1: Parallel Turbo-Downloading Zip to RAM...**\n"
                    f"`[{bar}]` {pct:.2f}%\n\n"
                    f"⚙️ **Progress:** {downloaded_bytes / (1024*1024):.2f} MB / {total_size / (1024*1024):.2f} MB\n"
                    f"⚡ **Speed:** {(speed / (1024*1024)):.2f} MB/s\n"
                    f"⏱️ **Elapsed Time:** {format_time(elapsed)}\n"
                    f"⏳ **Estimated Remaining (ETA):** {format_time(eta)}"
                )
                try:
                    await client.edit_message(event.chat_id, status_msg.id, status_text)
                except Exception:
                    pass
                last_update_time = now

    workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
    await asyncio.gather(*workers)
    
    return io.BytesIO(downloaded_buffer)

# --- CORE EVENT HANDLER ---
@client.on(events.NewMessage())
async def handle_userbot_media(event):
    message = event.message
    if not message.file:
        return

    filename = message.file.name or "unnamed_file"
    ext = os.path.splitext(filename)[1].lower() if '.' in filename else ""
    total_size = message.file.size

    if ext == '.zip':
        status_msg = await event.reply("📥 **Feature 1: Initializing Turbo Parallel Connections...**")
        try:
            buffer = await fast_parallel_download(client, message, status_msg, event)
            
            await client.edit_message(event.chat_id, status_msg.id, "⚙️ **Extracting layers and compiling deep file tree map...**")
            file_tree = extract_and_map_zip(buffer)
            
            final_output = f"📦 **Deep ZIP Extraction Map for:** `{filename}`\n\n"
            final_output += file_tree if file_tree.strip() else "📂 _(Archive empty)_"
            
            if len(final_output) > 4096:
                final_output = final_output[:4000] + "\n\n⚠️ *[Structure truncated]*"
            await client.edit_message(event.chat_id, status_msg.id, final_output)
            
            buffer.close()
            del buffer
            gc.collect()
            
        except Exception as e:
            await client.edit_message(event.chat_id, status_msg.id, f"❌ **Extraction failed:** `{str(e)}`")

    elif ext in ['.mp4', '.mkv', '.ts', '.mov', '.avi', '.webm', '.flv']:
        status_msg = await event.reply(f"🎬 **Feature 2: Initializing Pipe for:** `{filename}`")
        video_id = await get_bunny_video_id(filename, BUNNY_LIBRARY_ID, BUNNY_API_KEY)
        if not video_id:
            await client.edit_message(event.chat_id, status_msg.id, "❌ **Failed to set up container on Bunny Stream.**")
            return
            
        success = await piped_upload_to_bunny(client, message, video_id, total_size, BUNNY_LIBRARY_ID, BUNNY_API_KEY, status_msg)
        if success:
            await client.edit_message(event.chat_id, status_msg.id, f"✅ **Piped Upload Complete!**\n\n📛 **Name:** `{filename}`\n🆔 **ID:** `{video_id}`")
        else:
            await client.edit_message(event.chat_id, status_msg.id, f"❌ **Pipeline failed mid-stream for:** `{filename}`")

async def main():
    print("⚡ Userbot initializing connection...")
    await client.start()
    try:
        await client.send_message(
            'me', 
            "🚀 **Userbot Pipeline Status: LIVE**\n\n"
            "Parallel tracking engine fully corrected with auto-mapping location utilities. Try running it now!"
        )
        print("✅ Startup ping successfully dispatched.")
    except Exception as e:
        print(f"⚠️ Could not dispatch startup ping message: {e}")
    await client.run_until_disconnected()

if __name__ == '__main__':
    client.loop.run_until_complete(main())
