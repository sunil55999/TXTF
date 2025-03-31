import asyncio
import logging
import json
import re
from telethon import TelegramClient, events, errors
from collections import deque
from typing import Dict, List, Optional

# Your credentials
API_ID = 23697291
API_HASH = "b3a10e33ef507e864ed7018df0495ca8"

# Initialize the Telegram client
client = TelegramClient("userbot", API_ID, API_HASH)

# Configuration
MAPPINGS_FILE = "channel_mappings.json"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
MAX_QUEUE_SIZE = 100
MAX_MAPPING_HISTORY = 100

# Enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("forward_bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ForwardBot")

# Dictionary to store multiple source and destination mappings with names
channel_mappings: Dict[str, Dict] = {}
message_queue = deque(maxlen=MAX_QUEUE_SIZE)
is_connected = False

def save_mappings():
    try:
        with open(MAPPINGS_FILE, "w") as f:
            json.dump(channel_mappings, f)
        logger.info("Channel mappings saved to file.")
    except Exception as e:
        logger.error(f"Error saving mappings: {e}")

def load_mappings():
    global channel_mappings
    try:
        with open(MAPPINGS_FILE, "r") as f:
            channel_mappings = json.load(f)
        logger.info(f"Loaded {sum(len(v) for v in channel_mappings.values())} mappings from file.")
    except FileNotFoundError:
        logger.info("No existing mappings file found. Starting fresh.")
    except Exception as e:
        logger.error(f"Error loading mappings: {e}")

async def process_message_queue():
    """Process any queued messages when connection is restored."""
    while message_queue and is_connected:
        message_data = message_queue.popleft()
        await forward_message_with_retry(*message_data)

async def forward_message_with_retry(event, mapping, user_id, pair_name):
    """Forward a message with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            # Get message content
            message_text = event.message.text or event.message.raw_text or ""

            # Remove mentions if enabled
            if mapping['remove_mentions'] and message_text:
                message_text = re.sub(r'@[a-zA-Z0-9_]+|\[([^\]]+)\]\(tg://user\?id=\d+\)', '', message_text)
                message_text = re.sub(r'\s+', ' ', message_text).strip()
                logger.debug(f"Removed mentions from message: {message_text[:30]}...")

            # Handle reply preservation
            reply_to = await handle_reply_mapping(event, mapping)

            # Forward with media or without
            if event.message.media:
                sent_message = await client.send_message(
                    int(mapping['destination']),
                    message_text,
                    file=event.message.media,
                    reply_to=reply_to
                )
            else:
                sent_message = await client.send_message(
                    int(mapping['destination']),
                    message_text,
                    reply_to=reply_to
                )

            # Store the sent message for future reply mapping
            await store_message_mapping(event, mapping, sent_message)

            logger.info(f"Message forwarded from {mapping['source']} to {mapping['destination']}")
            return True

        except errors.FloodWaitError as e:
            wait_time = e.seconds
            logger.warning(f"Flood wait error, sleeping for {wait_time} seconds...")
            await asyncio.sleep(wait_time)
        except (errors.ConnectionError, errors.RPCError) as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY)
            else:
                logger.error(f"Failed to forward message after {MAX_RETRIES} attempts")
                return False
        except Exception as e:
            logger.error(f"Unexpected error forwarding message: {e}")
            return False

async def handle_reply_mapping(event, mapping) -> Optional[int]:
    """Handle reply message mapping between source and destination."""
    if not hasattr(event.message, 'reply_to') or not event.message.reply_to:
        return None

    try:
        source_reply_id = event.message.reply_to.reply_to_msg_id
        if not source_reply_id:
            return None

        logger.debug(f"Found reply to message ID: {source_reply_id}")

        # Check if we have this source message ID in our mapping
        mapping_key = f"{mapping['source']}:{source_reply_id}"
        if hasattr(client, 'forwarded_messages') and mapping_key in client.forwarded_messages:
            return client.forwarded_messages[mapping_key]

        # Fallback: search by content
        replied_msg = await client.get_messages(int(mapping['source']), ids=source_reply_id)
        if replied_msg and replied_msg.text:
            dest_msgs = await client.get_messages(
                int(mapping['destination']),
                search=replied_msg.text[:20],
                limit=5
            )
            if dest_msgs:
                return dest_msgs[0].id

    except Exception as e:
        logger.error(f"Error handling reply mapping: {e}")
    return None

async def store_message_mapping(event, mapping, sent_message):
    """Store the mapping between source and forwarded message IDs."""
    try:
        if not hasattr(event.message, 'id'):
            return

        if not hasattr(client, 'forwarded_messages'):
            client.forwarded_messages = {}

        # Clean up old mappings if we've reached the limit
        if len(client.forwarded_messages) >= MAX_MAPPING_HISTORY:
            oldest_key = next(iter(client.forwarded_messages))
            client.forwarded_messages.pop(oldest_key)

        # Add the new mapping
        source_msg_id = event.message.id
        mapping_key = f"{mapping['source']}:{source_msg_id}"
        client.forwarded_messages[mapping_key] = sent_message.id
        logger.debug(f"Stored message mapping: {mapping_key} -> {sent_message.id}")

    except Exception as e:
        logger.error(f"Error storing message mapping: {e}")

@client.on(events.NewMessage(pattern='(?i)^/start$'))
async def start(event):
    await event.reply("✅ Bot is running! Use /setpair to configure forwarding.")

@client.on(events.NewMessage(pattern='(?i)^/commands$'))
async def list_commands(event):
    commands = """
    📌 Available Commands:
    /setpair <name> <source> <destination> [remove_mentions]
    /listpairs - List all forwarding pairs
    /pausepair <name> - Pause a forwarding pair
    /startpair <name> - Resume a forwarding pair
    /clearpairs - Clear all forwarding pairs
    /togglementions <name> - Toggle mention removal for a forwarding pair
    """
    await event.reply(commands)

@client.on(events.NewMessage(pattern=r'/setpair (\S+) (\S+) (\S+)(?: (yes|no))?'))
async def set_pair(event):
    pair_name, source, destination, remove_mentions = event.pattern_match.groups()
    user_id = str(event.sender_id)
    remove_mentions = remove_mentions == "yes"

    if user_id not in channel_mappings:
        channel_mappings[user_id] = {}

    channel_mappings[user_id][pair_name] = {
        'source': source,
        'destination': destination,
        'active': True,
        'remove_mentions': remove_mentions
    }

    save_mappings()
    await event.reply(f"✅ Forwarding pair '{pair_name}' added: {source} → {destination} (Remove mentions: {remove_mentions})")

@client.on(events.NewMessage(pattern='(?i)^/togglementions (\S+)$'))
async def toggle_mentions(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        current_status = channel_mappings[user_id][pair_name]['remove_mentions']
        channel_mappings[user_id][pair_name]['remove_mentions'] = not current_status
        save_mappings()
        status_text = "ENABLED" if not current_status else "DISABLED"
        await event.reply(f"🔄 Mention removal {status_text} for forwarding pair '{pair_name}'.")
    else:
        await event.reply("⚠️ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/listpairs$'))
async def list_pairs(event):
    user_id = str(event.sender_id)
    if user_id in channel_mappings and channel_mappings[user_id]:
        pairs_list = "\n".join([
            f"{name}: {data['source']} → {data['destination']} (Active: {data['active']}, Remove Mentions: {data['remove_mentions']})"
            for name, data in channel_mappings[user_id].items()
        ])
        await event.reply(f"📋 Active Forwarding Pairs:\n{pairs_list}")
    else:
        await event.reply("⚠️ No forwarding pairs found.")

@client.on(events.NewMessage(pattern='(?i)^/pausepair (\S+)$'))
async def pause_pair(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['active'] = False
        save_mappings()
        await event.reply(f"⏸️ Forwarding pair '{pair_name}' has been paused.")
    else:
        await event.reply("⚠️ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/startpair (\S+)$'))
async def start_pair(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['active'] = True
        save_mappings()
        await event.reply(f"▶️ Forwarding pair '{pair_name}' has been activated.")
    else:
        await event.reply("⚠️ Pair not found.")

@client.on(events.NewMessage(pattern='(?i)^/clearpairs$'))
async def clear_pairs(event):
    user_id = str(event.sender_id)
    if user_id in channel_mappings:
        channel_mappings[user_id] = {}
        save_mappings()
        await event.reply("🗑️ All forwarding pairs have been cleared.")
    else:
        await event.reply("⚠️ No forwarding pairs found.")

@client.on(events.NewMessage)
async def forward_messages(event):
    if not is_connected:
        return

    for user_id, pairs in channel_mappings.items():
        for pair_name, mapping in pairs.items():
            if mapping['active'] and event.chat_id == int(mapping['source']):
                try:
                    success = await forward_message_with_retry(event, mapping, user_id, pair_name)
                    if not success:
                        message_queue.append((event, mapping, user_id, pair_name))
                        logger.warning(f"Message queued due to forwarding failure")
                except Exception as e:
                    logger.error(f"Error in forward_messages: {e}")
                    message_queue.append((event, mapping, user_id, pair_name))
                return

async def check_connection_status():
    """Periodically check and update connection status"""
    global is_connected
    while True:
        current_status = client.is_connected()
        if current_status and not is_connected:
            is_connected = True
            logger.info("Connection established, processing queued messages...")
            await process_message_queue()
        elif not current_status and is_connected:
            is_connected = False
            logger.warning("Connection lost, messages will be queued...")
        await asyncio.sleep(5)

async def main():
    load_mappings()

    # Start connection status checker
    asyncio.create_task(check_connection_status())

    logger.info("🚀 Bot is starting...")
    try:
        await client.start()
        # Set initial connection status
        global is_connected
        is_connected = client.is_connected()

        if is_connected:
            logger.info("Initial connection established")
        else:
            logger.warning("Initial connection not established")

        await client.run_until_disconnected()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        logger.info("Bot is shutting down...")
        save_mappings()

if __name__ == "__main__":
    try:
        client.loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
