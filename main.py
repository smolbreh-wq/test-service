import os
import discord
from discord.ext import commands
import asyncio
from keep_alive import keep_alive

import json
import aiofiles
from datetime import datetime
import re
from urllib.parse import urlparse

# ---------- CONFIG ----------
# Bot configurations: {token_env_name: prefix}
BOT_CONFIGS = {
    "TOKEN": "$",  # Main bot with $ prefix
    "TOKEN2": "!",  # Second bot with ! prefix  
    "TOKEN3": "?",  # Third bot with ? prefix
    # Add more bots as needed: "TOKEN4": "&", etc.
}

ALLOWED_USERS = [
    1096838620712804405,
    1348330851263315968,
    1414215242388344994  # replace with your Discord user ID (int)
    # Add more user IDs here as needed
]
MIN_DELAY = 0.5  # seconds
MAX_AMOUNT = 20

# Global variables to track spam tasks and stop flags for all bots
spam_tasks = {}
stop_flags = {}
bots = {}
emergency_stop = False

# Auto-restart functionality
last_commands = {
}  # Track last command per user per bot: {f"{prefix}_{user_id}": command_data}
restart_tasks = {}  # Track restart attempts
MAX_RESTART_ATTEMPTS = 3
RESTART_DELAY = 5  # seconds to wait before restart

# Account generation system
generated_accounts = {}  # Store generated account data
generation_tasks = {}  # Track ongoing generation tasks

# Dynamic bot management
dynamic_bots = {}  # Store dynamically added bots
DYNAMIC_BOTS_FILE = 'dynamic_bots.json'

# New features - Keyword listening system
listening_configs = {}  # Store listening configurations
listening_tasks = {}  # Track active listening tasks
LISTENING_CONFIGS_FILE = 'listening_configs.json'

# Message monitoring global variables
message_listeners = {}  # Store message event listeners per bot
# ---------------------------


def store_last_command(prefix: str, user_id: int, command_type: str, **kwargs):
    """Store the last executed command for auto-restart"""
    key = f"{prefix}_{user_id}"
    last_commands[key] = {
        'command_type': command_type,
        'timestamp': asyncio.get_event_loop().time(),
        'attempts': 0,
        **kwargs
    }


async def restart_last_command(ctx, prefix: str, error_msg: str = None):
    """Restart the last command after an error"""
    user_id = ctx.author.id
    key = f"{prefix}_{user_id}"

    if key not in last_commands:
        return False

    command_data = last_commands[key]
    command_data['attempts'] += 1

    # Don't restart if too many attempts
    if command_data['attempts'] > MAX_RESTART_ATTEMPTS:
        try:
            await ctx.author.send(
                f"❌ Max restart attempts ({MAX_RESTART_ATTEMPTS}) reached for {prefix} bot. "
                f"Please manually restart the command if needed.")
        except:
            pass
        last_commands.pop(key, None)
        return False

    try:
        await ctx.author.send(
            f"🔄 Restarting command on {prefix} bot (attempt {command_data['attempts']}/{MAX_RESTART_ATTEMPTS})"
            f"{f' after error: {error_msg}' if error_msg else ''}...")
    except:
        pass

    # Wait before restart
    await asyncio.sleep(RESTART_DELAY)

    # Restart based on command type
    try:
        if command_data['command_type'] == 'send':
            # Recreate send command
            message = command_data['message']
            delay = command_data['delay']
            amount = command_data['amount']

            # Don't store again to avoid infinite loop
            await execute_send_command(ctx,
                                       message,
                                       delay,
                                       amount,
                                       store_command=False)

        elif command_data['command_type'] == 'spm':
            # Recreate spm command
            message = command_data['message']
            delay = command_data['delay']

            # Don't store again to avoid infinite loop
            await execute_spm_command(ctx,
                                      prefix,
                                      message,
                                      delay,
                                      store_command=False)

        return True

    except Exception as e:
        try:
            await ctx.author.send(
                f"❌ Failed to restart command on {prefix} bot: {e}")
        except:
            pass
        return False


async def execute_send_command(ctx,
                               message: str,
                               delay: float,
                               amount: int,
                               store_command: bool = True):
    """Execute the send command with optional command storage"""
    prefix = ctx.bot.command_prefix
    user_id = ctx.author.id

    # Store command for auto-restart
    if store_command:
        store_last_command(prefix,
                           user_id,
                           'send',
                           message=message,
                           delay=delay,
                           amount=amount)

    # Create a stop flag for this user
    stop_flags[user_id] = False

    # Send the repeated messages
    for i in range(amount):
        # Check if user requested to stop or emergency stop activated
        if stop_flags.get(user_id, False) or emergency_stop:
            # Send stop notification to user's DM
            try:
                if emergency_stop:
                    await ctx.author.send(
                        f"🚨 Emergency stop activated. Message sending stopped after {i} messages."
                    )
                else:
                    await ctx.author.send(
                        f"🛑 Message sending stopped after {i} messages.")
            except:
                pass
            break

        try:
            await ctx.send(message)
            # Don't sleep after the last message
            if i < amount - 1:
                await asyncio.sleep(delay)

        # ---- 503 resilience (do not die; wait + continue) ----
        except discord.errors.DiscordServerError as e:
            if getattr(e, "status", None) == 503:
                try:
                    await ctx.author.send(
                        f"⚠️ Discord server error (503). Retrying…")
                except:
                    pass
                await asyncio.sleep(3)
                continue
            else:
                try:
                    await ctx.author.send(
                        f"⚠️ Discord server error occurred: {e}")
                except:
                    pass
                # Try to restart on non-503 server errors
                if await restart_last_command(ctx, prefix, str(e)):
                    return
                break

        except discord.HTTPException as e:
            # Keep original DM, but if it's specifically 503, don't stop
            try:
                await ctx.author.send(f"⚠️ Discord API error occurred: {e}")
            except:
                pass
            if getattr(e, "status", None) == 503:
                await asyncio.sleep(3)
                continue
            # Try to restart on non-503 HTTP errors
            if await restart_last_command(ctx, prefix, str(e)):
                return
            break
        # ------------------------------------------------------

        except Exception as e:
            # Send error to user's DM
            try:
                await ctx.author.send(f"⚠️ Unexpected error occurred: {e}")
            except:
                pass
            # Try to restart on unexpected errors
            if await restart_last_command(ctx, prefix, str(e)):
                return
            break

    # Clean up the stop flag
    stop_flags.pop(user_id, None)

    # Clear last command on successful completion
    key = f"{prefix}_{user_id}"
    if key in last_commands and last_commands[key]['command_type'] == 'send':
        last_commands.pop(key, None)


async def spam_loop_with_restart(ctx, message: str, delay: float, prefix: str):
    """Continuous spam loop with restart capability"""
    global emergency_stop
    count = 0
    user_id = ctx.author.id

    try:
        while True:
            if emergency_stop:
                try:
                    await ctx.author.send(
                        f"🚨 Emergency stop activated. Spam stopped after {count} messages."
                    )
                except:
                    pass
                break

            # ---- 503 resilience inside spam loop ----
            try:
                await ctx.send(message)
                count += 1
            except discord.errors.DiscordServerError as e:
                if getattr(e, "status", None) == 503:
                    await asyncio.sleep(3)
                    continue
                else:
                    # Try to restart on non-503 server errors
                    if await restart_last_command(ctx, prefix, str(e)):
                        return
                    raise
            except discord.HTTPException as e:
                if getattr(e, "status", None) == 503:
                    await asyncio.sleep(3)
                    continue
                else:
                    # Try to restart on non-503 HTTP errors
                    if await restart_last_command(ctx, prefix, str(e)):
                        return
                    raise
            except Exception as e:
                # Try to restart on unexpected errors
                if await restart_last_command(ctx, prefix, str(e)):
                    return
                raise
            # -----------------------------------------

            await asyncio.sleep(delay)
    except asyncio.CancelledError:
        try:
            await ctx.author.send(f"🛑 Spam stopped after {count} messages.")
        except:
            pass
        # Clear last command on cancellation
        key = f"{prefix}_{user_id}"
        if key in last_commands and last_commands[key]['command_type'] == 'spm':
            last_commands.pop(key, None)
        raise
    except Exception as e:
        try:
            await ctx.author.send(f"⚠️ Spam error after {count} messages: {e}")
        except:
            pass
        raise


async def execute_spm_command(ctx,
                              prefix: str,
                              message: str,
                              delay: float,
                              store_command: bool = True):
    """Execute the spm start command with optional command storage"""
    user_id = ctx.author.id

    # Store command for auto-restart
    if store_command:
        store_last_command(prefix,
                           user_id,
                           'spm',
                           message=message,
                           delay=delay)

    # Create unique key for this bot and user combination
    spam_key = f"{prefix}_{user_id}"

    # Stop any existing spam for this user on this specific bot
    if spam_key in spam_tasks:
        spam_tasks[spam_key].cancel()
        spam_tasks.pop(spam_key, None)

    # Start the spam task - notify user via DM
    if store_command:  # Only notify on original command, not restarts
        try:
            await ctx.author.send(
                f"🚀 Starting spam on {prefix} bot: '{message}' with {delay}s delay. Use `{prefix}stop` or `{prefix}spm stop` to stop."
            )
        except:
            pass

    task = asyncio.create_task(
        spam_loop_with_restart(ctx, message, delay, prefix))
    spam_tasks[spam_key] = task

    try:
        await task
    except asyncio.CancelledError:
        pass
    finally:
        spam_tasks.pop(spam_key, None)


def check_user_authorization(user_id: int, prefix: str = None) -> bool:
    """Check if user is authorized to use commands for a specific bot"""
    # Global ALLOWED_USERS always have access
    if user_id in ALLOWED_USERS:
        return True
    
    # Check bot-specific authorization
    if prefix and prefix in dynamic_bots:
        bot_data = dynamic_bots[prefix]
        # Bot's own user ID can use commands
        if user_id == bot_data.get('bot_user_id'):
            return True
        # Check bot-specific authorized users
        if user_id in bot_data.get('authorized_users', []):
            return True
    
    return False


async def save_dynamic_bots():
    """Save dynamic bots data to JSON file"""
    try:
        async with aiofiles.open(DYNAMIC_BOTS_FILE, 'w') as f:
            await f.write(json.dumps(dynamic_bots, indent=2))
    except Exception as e:
        print(f"Failed to save dynamic bots: {e}")


async def load_dynamic_bots():
    """Load dynamic bots data from JSON file"""
    global dynamic_bots
    try:
        async with aiofiles.open(DYNAMIC_BOTS_FILE, 'r') as f:
            content = await f.read()
            dynamic_bots = json.loads(content)
    except FileNotFoundError:
        dynamic_bots = {}
    except Exception as e:
        print(f"Failed to load dynamic bots: {e}")
        dynamic_bots = {}


async def save_listening_configs():
    """Save listening configurations to JSON file"""
    try:
        async with aiofiles.open(LISTENING_CONFIGS_FILE, 'w') as f:
            await f.write(json.dumps(listening_configs, indent=2))
    except Exception as e:
        print(f"Failed to save listening configs: {e}")


async def load_listening_configs():
    """Load listening configurations from JSON file"""
    global listening_configs
    try:
        async with aiofiles.open(LISTENING_CONFIGS_FILE, 'r') as f:
            content = await f.read()
            listening_configs = json.loads(content)
    except FileNotFoundError:
        listening_configs = {}
    except Exception as e:
        print(f"Failed to load listening configs: {e}")
        listening_configs = {}


def parse_channel_link(channel_link: str):
    """Parse Discord channel link and extract channel ID"""
    try:
        # Handle different Discord URL formats
        # https://discord.com/channels/server_id/channel_id
        # https://discord.com/channels/server_id/channel_id/message_id
        # https://discordapp.com/channels/server_id/channel_id
        
        if 'discord' in channel_link and 'channels' in channel_link:
            parts = channel_link.split('/')
            if len(parts) >= 6:
                server_id = int(parts[4])
                channel_id = int(parts[5])
                return server_id, channel_id
        
        # Handle direct channel ID
        if channel_link.isdigit():
            return None, int(channel_link)
            
    except (ValueError, IndexError):
        pass
    return None, None


def parse_message_link(message_link: str):
    """Parse Discord message link and extract server_id, channel_id, message_id"""
    try:
        # https://discord.com/channels/server_id/channel_id/message_id
        # https://discordapp.com/channels/server_id/channel_id/message_id
        
        if 'discord' in message_link and 'channels' in message_link:
            parts = message_link.split('/')
            if len(parts) >= 7:
                server_id = int(parts[4]) if parts[4] != '@me' else None
                channel_id = int(parts[5])
                message_id = int(parts[6])
                return server_id, channel_id, message_id
        
        # Handle direct message ID
        if message_link.isdigit():
            return None, None, int(message_link)
            
    except (ValueError, IndexError):
        pass
    return None, None, None


def check_keywords_match(message_content: str, keywords: list, case_sensitive: bool, word_match: bool):
    """Check if message content matches any of the keywords"""
    content = message_content if case_sensitive else message_content.lower()
    
    for keyword in keywords:
        keyword = keyword.strip()
        if not case_sensitive:
            keyword = keyword.lower()
        
        if word_match:
            # Match whole words only
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, content):
                return True, keyword
        else:
            # Match anywhere in the message
            if keyword in content:
                return True, keyword
    
    return False, None


async def setup_keyword_listener(bot, channel_id: int, config: dict):
    """Set up keyword listener for a specific channel"""
    try:
        channel = bot.get_channel(channel_id)
        if not channel:
            # Try to fetch the channel
            channel = await bot.fetch_channel(channel_id)
        
        if not channel:
            return False, "Channel not found or bot doesn't have access"
        
        # Store the listener config
        listener_key = f"{bot.command_prefix}_{channel_id}"
        listening_configs[listener_key] = config
        await save_listening_configs()
        
        return True, f"Keyword listener set up for #{channel.name}"
    
    except discord.Forbidden:
        return False, "Bot doesn't have permission to access this channel"
    except discord.NotFound:
        return False, "Channel not found"
    except Exception as e:
        return False, f"Error setting up listener: {str(e)}"


async def handle_keyword_message(message, bot_prefix):
    """Handle incoming message for keyword matching"""
    # Skip bot messages
    if message.author.bot:
        return
    
    # Check all listening configs for this bot
    for listener_key, config in listening_configs.items():
        if not listener_key.startswith(f"{bot_prefix}_"):
            continue
        
        channel_id = int(listener_key.split('_', 1)[1])
        if message.channel.id != channel_id:
            continue
        
        # Check for keyword match
        keywords = config['keywords']
        case_sensitive = config['case_sensitive']
        word_match = config['word_match']
        user_id = config['user_id']
        
        match_found, matched_keyword = check_keywords_match(
            message.content, keywords, case_sensitive, word_match)
        
        if match_found:
            # Create message link
            message_link = f"https://discord.com/channels/{message.guild.id if message.guild else '@me'}/{message.channel.id}/{message.id}"
            
            # Send DM to the user who set up the listener
            try:
                user = await message.guild.members[0]._state.http.get_user(user_id) if message.guild else None
                if not user:
                    # Try to get user from any bot
                    for bot in bots.values():
                        try:
                            user = await bot.fetch_user(user_id)
                            break
                        except:
                            continue
                
                if user:
                    dm_content = f"🔍 **Keyword Alert**\n"
                    dm_content += f"**Matched Keyword:** {matched_keyword}\n"
                    dm_content += f"**Author:** {message.author.mention} ({message.author})\n"
                    dm_content += f"**Channel:** #{message.channel.name}\n"
                    dm_content += f"**Message:** {message.content}\n"
                    dm_content += f"**Link:** {message_link}"
                    
                    await user.send(dm_content)
            except Exception as e:
                print(f"Failed to send keyword alert DM: {e}")


async def multi_bot_react(emojis: list, num_reactions: int, message_link: str, delay: float, author_id: int):
    """Add reactions using multiple bots"""
    server_id, channel_id, message_id = parse_message_link(message_link)
    
    if not message_id:
        return False, "Invalid message link or message ID"
    
    # Get all active bots
    active_bots = list(bots.values())
    if not active_bots:
        return False, "No active bots available"
    
    reactions_added = 0
    errors = []
    
    try:
        # Distribute reactions across bots
        for i in range(num_reactions):
            bot = active_bots[i % len(active_bots)]
            emoji = emojis[i % len(emojis)]
            
            try:
                # Get the message
                if channel_id:
                    channel = bot.get_channel(channel_id)
                    if not channel:
                        channel = await bot.fetch_channel(channel_id)
                    message = await channel.fetch_message(message_id)
                else:
                    # Try to find message in accessible channels
                    message = None
                    for guild in bot.guilds:
                        for channel in guild.text_channels:
                            try:
                                message = await channel.fetch_message(message_id)
                                break
                            except:
                                continue
                        if message:
                            break
                
                if not message:
                    errors.append(f"Message not found for bot {bot.command_prefix}")
                    continue
                
                # Add reaction
                await message.add_reaction(emoji)
                reactions_added += 1
                
                # Add delay between reactions
                if delay > 0 and i < num_reactions - 1:
                    await asyncio.sleep(delay)
                
            except discord.Forbidden:
                errors.append(f"Bot {bot.command_prefix} lacks permission to react")
            except discord.NotFound:
                errors.append(f"Message/channel not found for bot {bot.command_prefix}")
            except discord.HTTPException as e:
                errors.append(f"Bot {bot.command_prefix} reaction error: {str(e)}")
            except Exception as e:
                errors.append(f"Bot {bot.command_prefix} unexpected error: {str(e)}")
    
    except Exception as e:
        return False, f"Multi-bot reaction failed: {str(e)}"
    
    # Send result to user
    try:
        # Get user from any bot
        user = None
        for bot in active_bots:
            try:
                user = await bot.fetch_user(author_id)
                break
            except:
                continue
        
        if user:
            result_msg = f"✅ Added {reactions_added}/{num_reactions} reactions"
            if errors:
                result_msg += f"\n❌ Errors: {len(errors)}"
                for error in errors[:5]:  # Show first 5 errors
                    result_msg += f"\n• {error}"
            await user.send(result_msg)
    except:
        pass
    
    return True, f"Added {reactions_added}/{num_reactions} reactions"


def get_prefix_owner(prefix: str) -> str:
    """Get which bot/system owns a specific prefix"""
    # Check hardcoded bots
    for token_name, bot_prefix in BOT_CONFIGS.items():
        if bot_prefix == prefix:
            return f"Hardcoded bot ({token_name})"
    
    # Check dynamic bots
    if prefix in dynamic_bots:
        return f"Dynamic bot (ID: {dynamic_bots[prefix].get('bot_user_id', 'Unknown')})"
    
    # Check system prefix
    if prefix == ">":
        return "System commands"
    
    return None


async def stop_bot_tasks(prefix: str):
    """Stop all tasks for a specific bot"""
    tasks_stopped = 0
    
    # Stop spam tasks for this bot
    keys_to_remove = []
    for spam_key in spam_tasks:
        if spam_key.startswith(f"{prefix}_"):
            spam_tasks[spam_key].cancel()
            keys_to_remove.append(spam_key)
            tasks_stopped += 1
    
    for key in keys_to_remove:
        spam_tasks.pop(key, None)
    
    # Stop regular send commands by setting stop flags
    for user_id in stop_flags:
        stop_flags[user_id] = True
    
    # Clear last commands for this bot to prevent auto-restart
    last_command_keys_to_remove = []
    for key in last_commands:
        if key.startswith(f"{prefix}_"):
            last_command_keys_to_remove.append(key)
    
    for key in last_command_keys_to_remove:
        last_commands.pop(key, None)
    
    # Stop listening tasks for this bot
    listening_keys_to_remove = []
    for listener_key in listening_configs:
        if listener_key.startswith(f"{prefix}_"):
            listening_keys_to_remove.append(listener_key)
            tasks_stopped += 1
    
    for key in listening_keys_to_remove:
        listening_configs.pop(key, None)
    
    await save_listening_configs()
    
    return tasks_stopped


def create_bot(prefix: str, bot_name: str):
    """Create a bot instance with the given prefix"""
    # Using discord.py-self - no intents or self_bot needed
    bot = commands.Bot(command_prefix=prefix)

    @bot.event
    async def on_ready():
        print(f"✅ {bot_name} logged in as {bot.user} (ID: {bot.user.id})")
        print(
            f"Bot is ready and listening for commands with prefix '{prefix}'")
        
        # Store bot user ID for dynamic bots
        if prefix in dynamic_bots:
            dynamic_bots[prefix]['bot_user_id'] = bot.user.id
            await save_dynamic_bots()

    @bot.event
    async def on_message(message):
        """Handle system commands, keyword monitoring, and regular commands"""
        global emergency_stop

        # Handle keyword listening
        await handle_keyword_message(message, prefix)

        # Ignore messages from bots for command processing
        if message.author.bot:
            return

        user_id = message.author.id
        content = message.content

        # System commands (prefix: >)
        if content.startswith(">") and user_id in ALLOWED_USERS:
            
            # Emergency stopall command
            if content == ">stopall":
                emergency_stop = True

                # Cancel all active spam tasks
                for spam_key, task in list(spam_tasks.items()):
                    task.cancel()
                    spam_tasks.pop(spam_key, None)

                # Set all stop flags
                for user_id_flag in list(stop_flags.keys()):
                    stop_flags[user_id_flag] = True

                # Clear all last commands to prevent auto-restart
                last_commands.clear()

                try:
                    await message.author.send(
                        "🚨 EMERGENCY STOP ACTIVATED - All bots stopped! Auto-restart disabled for all commands."
                    )
                except:
                    pass

                # Reset emergency stop after a brief moment to allow for new commands
                await asyncio.sleep(1)
                emergency_stop = False
                return

            # Show all bots command
            elif content == ">showallbots":
                bot_list = []
                
                # Add hardcoded bots
                for token_name, bot_prefix in BOT_CONFIGS.items():
                    if bot_prefix in bots:
                        bot_list.append(f"**{bot_prefix}** - Hardcoded ({token_name})")
                
                # Add dynamic bots
                for prefix_name, bot_data in dynamic_bots.items():
                    if prefix_name in bots:
                        bot_id = bot_data.get('bot_user_id', 'Unknown')
                        bot_list.append(f"**{prefix_name}** - Dynamic (ID: {bot_id})")
                
                if bot_list:
                    bot_list_str = "\n".join(bot_list)
                    response = f"🤖 **Active Bots ({len(bot_list)}):**\n{bot_list_str}"
                else:
                    response = "ℹ️ No active bots found."
                
                try:
                    await message.author.send(response)
                except:
                    pass
                return

            # Multi-bot reaction command
            elif content.startswith(">react"):
                parts = content.split(" ", 3)
                if len(parts) < 4:
                    try:
                        await message.author.send("Usage: `>react {emoji1,emoji2} {num_reactions} {message_link} [delay]`")
                    except:
                        pass
                    return
                
                try:
                    emoji_str = parts[1]
                    num_reactions = int(parts[2])
                    message_link = parts[3]
                    delay = 1.0  # default delay
                    
                    # Parse optional delay
                    if len(parts) > 4:
                        delay = float(parts[4])
                    
                    # Parse emojis
                    emojis = [emoji.strip() for emoji in emoji_str.split(',')]
                    
                    if num_reactions <= 0 or num_reactions > 100:
                        try:
                            await message.author.send("❌ Number of reactions must be between 1 and 100")
                        except:
                            pass
                        return
                    
                    # Start multi-bot reaction task
                    asyncio.create_task(multi_bot_react(emojis, num_reactions, message_link, delay, user_id))
                    
                    try:
                        await message.author.send(f"🚀 Starting multi-bot reactions: {num_reactions} reactions with {delay}s delay")
                    except:
                        pass
                
                except ValueError:
                    try:
                        await message.author.send("❌ Invalid number format. Use: `>react {emojis} {number} {message_link} [delay]`")
                    except:
                        pass
                except Exception as e:
                    try:
                        await message.author.send(f"❌ Reaction command error: {str(e)}")
                    except:
                        pass
                return

            # Add bot command
            elif content.startswith(">addbot"):
                parts = content.split(" ", 2)
                if len(parts) != 3:
                    try:
                        await message.author.send("Usage: `>addbot [prefix] [token]`")
                    except:
                        pass
                    return
                
                new_prefix = parts[1]
                new_token = parts[2]
                
                # Check if prefix is already in use
                existing_owner = get_prefix_owner(new_prefix)
                if existing_owner:
                    try:
                        await message.author.send(f"❌ Prefix '{new_prefix}' is already in use by: {existing_owner}")
                    except:
                        pass
                    return
                
                # Check if it's system prefix
                if new_prefix == ">":
                    try:
                        await message.author.send("❌ Prefix '>' is reserved for system commands.")
                    except:
                        pass
                    return
                
                try:
                    # Create bot data
                    bot_data = {
                        'token': new_token,
                        'prefix': new_prefix,
                        'authorized_users': [],
                        'bot_user_id': None,  # Will be set when bot connects
                        'created_by': user_id,
                        'created_at': datetime.now().isoformat()
                    }
                    
                    # Add to dynamic bots
                    dynamic_bots[new_prefix] = bot_data
                    await save_dynamic_bots()
                    
                    # Create and start bot
                    bot_name = f"DynamicBot-{new_prefix}"
                    new_bot = create_bot(new_prefix, bot_name)
                    bots[new_prefix] = new_bot
                    
                    # Start bot in background
                    asyncio.create_task(new_bot.start(new_token))
                    
                    try:
                        await message.author.send(f"✅ Bot with prefix '{new_prefix}' added successfully! Starting bot...")
                    except:
                        pass
                    
                except Exception as e:
                    try:
                        await message.author.send(f"❌ Failed to add bot: {str(e)}")
                    except:
                        pass
                return

            # Remove bot command
            elif content.startswith(">removebot"):
                parts = content.split(" ", 1)
                if len(parts) != 2:
                    try:
                        await message.author.send("Usage: `>removebot [prefix]`")
                    except:
                        pass
                    return
                
                target_prefix = parts[1]
                
                # Check if bot exists
                if target_prefix not in dynamic_bots:
                    try:
                        await message.author.send(f"❌ No dynamic bot found with prefix '{target_prefix}'")
                    except:
                        pass
                    return
                
                try:
                    # Stop all tasks for this bot
                    tasks_stopped = await stop_bot_tasks(target_prefix)
                    
                    # Logout and remove bot
                    if target_prefix in bots:
                        try:
                            await bots[target_prefix].close()
                        except:
                            pass
                        bots.pop(target_prefix, None)
                    
                    # Remove from dynamic bots
                    dynamic_bots.pop(target_prefix, None)
                    await save_dynamic_bots()
                    
                    try:
                        await message.author.send(f"✅ Bot with prefix '{target_prefix}' removed successfully! Stopped {tasks_stopped} active tasks.")
                    except:
                        pass
                    
                except Exception as e:
                    try:
                        await message.author.send(f"❌ Failed to remove bot: {str(e)}")
                    except:
                        pass
                return

            # Change prefix command for dynamic bots
            elif content.startswith(">changeprefix"):
                parts = content.split(" ", 2)
                if len(parts) != 3:
                    try:
                        await message.author.send("Usage: `>changeprefix [old_prefix] [new_prefix]`")
                    except:
                        pass
                    return
                
                old_prefix = parts[1]
                new_prefix = parts[2]
                
                # Check if old bot exists in dynamic bots
                if old_prefix not in dynamic_bots:
                    try:
                        await message.author.send(f"❌ No dynamic bot found with prefix '{old_prefix}'")
                    except:
                        pass
                    return
                
                # Check if new prefix is already in use
                existing_owner = get_prefix_owner(new_prefix)
                if existing_owner:
                    try:
                        await message.author.send(f"❌ New prefix '{new_prefix}' is already in use by: {existing_owner}")
                    except:
                        pass
                    return
                
                # Check if it's system prefix
                if new_prefix == ">":
                    try:
                        await message.author.send("❌ Prefix '>' is reserved for system commands.")
                    except:
                        pass
                    return
                
                try:
                    # Get bot data and token
                    bot_data = dynamic_bots[old_prefix]
                    token = bot_data['token']
                    
                    # Stop and remove old bot
                    tasks_stopped = await stop_bot_tasks(old_prefix)
                    
                    if old_prefix in bots:
                        try:
                            await bots[old_prefix].close()
                        except:
                            pass
                        bots.pop(old_prefix, None)
                    
                    # Update bot data with new prefix
                    bot_data['prefix'] = new_prefix
                    
                    # Remove old entry and add new one
                    dynamic_bots.pop(old_prefix, None)
                    dynamic_bots[new_prefix] = bot_data
                    await save_dynamic_bots()
                    
                    # Create and start bot with new prefix
                    bot_name = f"DynamicBot-{new_prefix}"
                    new_bot = create_bot(new_prefix, bot_name)
                    bots[new_prefix] = new_bot
                    
                    # Start bot with new prefix
                    asyncio.create_task(new_bot.start(token))
                    
                    try:
                        await message.author.send(f"✅ Bot prefix changed from '{old_prefix}' to '{new_prefix}' successfully! Stopped {tasks_stopped} tasks from old bot.")
                    except:
                        pass
                    
                except Exception as e:
                    try:
                        await message.author.send(f"❌ Failed to change prefix: {str(e)}")
                    except:
                        pass
                return

            # Add user command
            elif content.startswith(">adduser"):
                parts = content.split(" ", 2)
                if len(parts) != 3:
                    try:
                        await message.author.send("Usage: `>adduser [user_ID] [prefix]`")
                    except:
                        pass
                    return
                
                try:
                    target_user_id = int(parts[1])
                    target_prefix = parts[2]
                except ValueError:
                    try:
                        await message.author.send("❌ Invalid user ID. Must be a number.")
                    except:
                        pass
                    return
                
                # Check if bot exists
                if target_prefix not in dynamic_bots:
                    try:
                        await message.author.send(f"❌ No dynamic bot found with prefix '{target_prefix}'")
                    except:
                        pass
                    return
                
                # Check if user is already authorized
                if target_user_id in dynamic_bots[target_prefix]['authorized_users']:
                    try:
                        await message.author.send(f"⚠️ User {target_user_id} is already authorized for bot '{target_prefix}'")
                    except:
                        pass
                    return
                
                # Add user to authorized list
                dynamic_bots[target_prefix]['authorized_users'].append(target_user_id)
                await save_dynamic_bots()
                
                try:
                    await message.author.send(f"✅ User {target_user_id} added to authorized users for bot '{target_prefix}'")
                except:
                    pass
                return

            # Remove user command
            elif content.startswith(">removeuser"):
                parts = content.split(" ", 2)
                if len(parts) != 3:
                    try:
                        await message.author.send("Usage: `>removeuser [user_ID] [prefix]`")
                    except:
                        pass
                    return
                
                try:
                    target_user_id = int(parts[1])
                    target_prefix = parts[2]
                except ValueError:
                    try:
                        await message.author.send("❌ Invalid user ID. Must be a number.")
                    except:
                        pass
                    return
                
                # Check if bot exists
                if target_prefix not in dynamic_bots:
                    try:
                        await message.author.send(f"❌ No dynamic bot found with prefix '{target_prefix}'")
                    except:
                        pass
                    return
                
                # Check if user is in authorized list
                if target_user_id not in dynamic_bots[target_prefix]['authorized_users']:
                    try:
                        await message.author.send(f"⚠️ User {target_user_id} is not in authorized users for bot '{target_prefix}'")
                    except:
                        pass
                    return
                
                # Remove user from authorized list
                dynamic_bots[target_prefix]['authorized_users'].remove(target_user_id)
                await save_dynamic_bots()
                
                try:
                    await message.author.send(f"✅ User {target_user_id} removed from authorized users for bot '{target_prefix}'")
                except:
                    pass
                return

        # Check for account generation command
        if content.startswith(">generate account") and user_id in ALLOWED_USERS:
            await handle_account_generation(message)
            return

        # Debug: Show when processing commands
        if any(content.startswith(prefix) for prefix in [prefix for prefix in BOT_CONFIGS.values()] + list(dynamic_bots.keys()) + ['>']):
            if check_user_authorization(user_id, content[0] if content else None):
                print(f"🔍 Processing command: '{content}' from user {user_id}")
        
        # Process normal commands
        await bot.process_commands(message)

    @bot.check
    async def is_allowed(ctx):
        """Global check to ensure only authorized users can use bot commands"""
        is_authorized = check_user_authorization(ctx.author.id, prefix)
        # Debug: Print user ID for troubleshooting
        if not is_authorized:
            print(f"❌ Unauthorized user tried command: {ctx.author.id} on bot {prefix}")
        else:
            print(f"✅ Authorized user {ctx.author.id} using command: {ctx.command} on bot {prefix}")
        return is_authorized

    @bot.command()
    async def send(ctx, message: str, delay: float, amount: int):
        """
        Send a message multiple times with a specified delay between each message.

        Usage: {prefix}send [message] [delay] [amount]
        Example: {prefix}send "Hello World" 1.0 5

        Parameters:
        - message: The message to send (use quotes for multi-word messages)
        - delay: Delay in seconds between messages (minimum 0.5 seconds)
        - amount: Number of times to send the message (1-20)
        """
        try:
            # Validate delay parameter
            if delay < MIN_DELAY:
                try:
                    await ctx.author.send(
                        f"⚠️ Delay must be at least {MIN_DELAY} seconds to prevent rate limiting."
                    )
                except:
                    pass
                return

            # Validate amount parameter
            if amount < 1 or amount > MAX_AMOUNT:
                try:
                    await ctx.author.send(
                        f"⚠️ Amount must be between 1 and {MAX_AMOUNT} messages."
                    )
                except:
                    pass
                return

            # Validate message length (Discord has a 2000 character limit)
            if len(message) > 2000:
                try:
                    await ctx.author.send(
                        "⚠️ Message is too long. Discord messages must be 2000 characters or less."
                    )
                except:
                    pass
                return

            # Delete the invoking command message to keep chat clean
            try:
                await ctx.message.delete()
            except discord.Forbidden:
                pass
            except Exception:
                pass

            await execute_send_command(ctx, message, delay, amount)

        except ValueError:
            try:
                await ctx.author.send(
                    f"⚠️ Invalid parameters. Please use: `{prefix}send [message] [delay] [amount]`\nExample: `{prefix}send \"Hello\" 1.0 5`"
                )
            except:
                pass
        except Exception as e:
            try:
                await ctx.author.send(f"⚠️ Error processing command: {e}")
            except:
                pass

    @bot.command()
    async def stop(ctx):
        """
        Stop any ongoing message sending for the user.

        Usage: {prefix}stop
        """
        user_id = ctx.author.id

        # Stop regular send command
        if user_id in stop_flags:
            stop_flags[user_id] = True
            try:
                await ctx.author.send("🛑 Stopping message sending...")
            except:
                pass

        # Stop spam command for this specific bot
        spam_key = f"{prefix}_{user_id}"
        if spam_key in spam_tasks:
            spam_tasks[spam_key].cancel()
            spam_tasks.pop(spam_key, None)
            try:
                await ctx.author.send(
                    f"🛑 Spam sending stopped on {prefix} bot.")
            except:
                pass

        # Clear last command to prevent auto-restart
        last_command_key = f"{prefix}_{user_id}"
        if last_command_key in last_commands:
            last_commands.pop(last_command_key, None)
            try:
                await ctx.author.send(
                    f"🛑 Auto-restart disabled for {prefix} bot.")
            except:
                pass

        if user_id not in stop_flags and spam_key not in spam_tasks:
            try:
                await ctx.author.send(
                    f"ℹ️ No active message sending to stop on {prefix} bot.")
            except:
                pass

    @bot.command()
    async def spm(ctx, action: str, message: str = None, delay: float = 1.0):
        """
        Continuous spam command with start/stop functionality.

        Usage: {prefix}spm start [message] [delay]
               {prefix}spm stop

        Examples:
        {prefix}spm start "Hello" 1.0
        {prefix}spm stop
        """
        user_id = ctx.author.id

        if action.lower() == "start":
            if not message:
                try:
                    await ctx.author.send(
                        f"⚠️ Please provide a message to spam.\nUsage: `{prefix}spm start \"message\" [delay]`"
                    )
                except:
                    pass
                return

            if delay < MIN_DELAY:
                try:
                    await ctx.author.send(
                        f"⚠️ Delay must be at least {MIN_DELAY} seconds.")
                except:
                    pass
                return

            # Delete the command message
            try:
                await ctx.message.delete()
            except Exception:
                pass

            await execute_spm_command(ctx, prefix, message, delay)

        elif action.lower() == "stop":
            spam_key = f"{prefix}_{user_id}"
            if spam_key in spam_tasks:
                spam_tasks[spam_key].cancel()
                spam_tasks.pop(spam_key, None)
                try:
                    await ctx.author.send(f"🛑 Spam stopped on {prefix} bot.")
                except:
                    pass

            # Clear last command to prevent auto-restart
            last_command_key = f"{prefix}_{user_id}"
            if last_command_key in last_commands:
                last_commands.pop(last_command_key, None)
                try:
                    await ctx.author.send(
                        f"🛑 Auto-restart disabled for {prefix} bot.")
                except:
                    pass

            if spam_key not in spam_tasks:
                try:
                    await ctx.author.send(
                        f"ℹ️ No active spam to stop on {prefix} bot.")
                except:
                    pass

        else:
            try:
                await ctx.author.send(
                    f"⚠️ Invalid action. Use `start` or `stop`.\nExample: `{prefix}spm start \"message\" 1.0`"
                )
            except:
                pass

    @bot.command()
    async def restart(ctx):
        """
        Manually restart the last command that was running on this bot.

        Usage: {prefix}restart
        """
        user_id = ctx.author.id
        key = f"{prefix}_{user_id}"

        if key not in last_commands:
            try:
                await ctx.author.send(
                    f"ℹ️ No previous command to restart on {prefix} bot.")
            except:
                pass
            return

        # Reset attempt counter for manual restart
        last_commands[key]['attempts'] = 0

        try:
            await ctx.author.send(
                f"🔄 Manually restarting last command on {prefix} bot...")
        except:
            pass

        await restart_last_command(ctx, prefix)

    @bot.command()
    async def listento(ctx, case_sensitive: str, word_match: str, keywords: str, channel_link: str):
        """
        Start listening for keywords in a channel.
        
        Usage: {prefix}listento [y/n] [y/n] "keyword1,keyword2" [channel_link]
        Example: {prefix}listento y n "hello,world" https://discord.com/channels/123/456
        
        Parameters:
        - case_sensitive: y for case-sensitive, n for case-insensitive
        - word_match: y for whole words only, n for partial matches
        - keywords: Comma-separated keywords in quotes
        - channel_link: Discord channel link or channel ID
        """
        user_id = ctx.author.id
        
        # Parse case sensitivity
        case_sensitive = case_sensitive.lower() == 'y'
        word_match = word_match.lower() == 'y'
        
        # Parse keywords
        keyword_list = [kw.strip() for kw in keywords.split(',')]
        
        # Parse channel link
        server_id, channel_id = parse_channel_link(channel_link)
        
        if not channel_id:
            try:
                await ctx.author.send("❌ Invalid channel link or channel ID")
            except:
                pass
            return
        
        # Set up keyword listener config
        config = {
            'user_id': user_id,
            'keywords': keyword_list,
            'case_sensitive': case_sensitive,
            'word_match': word_match,
            'channel_id': channel_id,
            'created_at': datetime.now().isoformat()
        }
        
        success, message = await setup_keyword_listener(bot, channel_id, config)
        
        try:
            if success:
                settings_str = f"Case-sensitive: {'Yes' if case_sensitive else 'No'}, Word match: {'Yes' if word_match else 'No'}"
                await ctx.author.send(f"✅ {message}\n**Keywords:** {', '.join(keyword_list)}\n**Settings:** {settings_str}")
            else:
                await ctx.author.send(f"❌ Failed to set up listener: {message}")
        except:
            pass

    @bot.command()
    async def stoplisten(ctx, channel_link: str):
        """
        Stop listening for keywords in a channel.
        
        Usage: {prefix}stoplisten [channel_link]
        Example: {prefix}stoplisten https://discord.com/channels/123/456
        """
        user_id = ctx.author.id
        
        # Parse channel link
        server_id, channel_id = parse_channel_link(channel_link)
        
        if not channel_id:
            try:
                await ctx.author.send("❌ Invalid channel link or channel ID")
            except:
                pass
            return
        
        # Remove listener config
        listener_key = f"{prefix}_{channel_id}"
        
        if listener_key in listening_configs:
            # Check if user owns this listener
            if listening_configs[listener_key]['user_id'] != user_id:
                try:
                    await ctx.author.send("❌ You can only stop listeners you created")
                except:
                    pass
                return
            
            listening_configs.pop(listener_key, None)
            await save_listening_configs()
            
            try:
                await ctx.author.send(f"✅ Stopped listening for keywords in the specified channel")
            except:
                pass
        else:
            try:
                await ctx.author.send(f"ℹ️ No active listener found for the specified channel")
            except:
                pass

    @bot.command()
    async def editlisten(ctx, case_sensitive: str, word_match: str, keywords: str, channel_link: str):
        """
        Edit keyword listener settings for a channel.
        
        Usage: {prefix}editlisten [y/n] [y/n] "keyword1,keyword2" [channel_link]
        Example: {prefix}editlisten n y "test,demo" https://discord.com/channels/123/456
        """
        user_id = ctx.author.id
        
        # Parse parameters
        case_sensitive = case_sensitive.lower() == 'y'
        word_match = word_match.lower() == 'y'
        keyword_list = [kw.strip() for kw in keywords.split(',')]
        
        # Parse channel link
        server_id, channel_id = parse_channel_link(channel_link)
        
        if not channel_id:
            try:
                await ctx.author.send("❌ Invalid channel link or channel ID")
            except:
                pass
            return
        
        # Check if listener exists
        listener_key = f"{prefix}_{channel_id}"
        
        if listener_key not in listening_configs:
            try:
                await ctx.author.send("❌ No listener found for this channel. Use `listento` to create one first")
            except:
                pass
            return
        
        # Check if user owns this listener
        if listening_configs[listener_key]['user_id'] != user_id:
            try:
                await ctx.author.send("❌ You can only edit listeners you created")
            except:
                pass
            return
        
        # Update listener config
        listening_configs[listener_key].update({
            'keywords': keyword_list,
            'case_sensitive': case_sensitive,
            'word_match': word_match,
            'updated_at': datetime.now().isoformat()
        })
        
        await save_listening_configs()
        
        try:
            settings_str = f"Case-sensitive: {'Yes' if case_sensitive else 'No'}, Word match: {'Yes' if word_match else 'No'}"
            await ctx.author.send(f"✅ Updated keyword listener\n**Keywords:** {', '.join(keyword_list)}\n**Settings:** {settings_str}")
        except:
            pass

    @bot.command()
    async def help_bot(ctx, category: str = None, command: str = None):
        """Display help information about bot commands"""
        
        # If specific command help is requested
        if command:
            part1, part2 = get_command_help(command, prefix)
            if part1 and "not found" not in part1:
                await ctx.send(part1)
                if part2:
                    await asyncio.sleep(1)
                    await ctx.send(part2)
            else:
                await ctx.send(f"❌ Command `{command}` not found")
            return
        
        # If category help is requested
        if category:
            category_help = get_category_help(category, prefix)
            if category_help:
                await ctx.send(category_help)
            else:
                await ctx.send(f"❌ Category `{category}` not found\nAvailable: basic, listening, system, management")
            return
        
        # Main help menu
        help_message = f"""**Bot Help Menu** (Prefix: {prefix})

**Categories:**
`{prefix}help_bot basic` - Message sending & spam commands
`{prefix}help_bot listening` - Keyword monitoring commands  
`{prefix}help_bot system` - System-wide commands (>)
`{prefix}help_bot management` - Bot management commands (>)

**Quick Help:**
`{prefix}help_bot [command]` - Show specific command usage
Example: `{prefix}help_bot send`

**Emergency:** `>stopall` - Stop everything immediately"""

        await ctx.send(help_message)


def get_category_help(category: str, prefix: str) -> str:
    """Get help for a specific category"""
    
    if category.lower() == "basic":
        return f"""**Basic Commands** (Prefix: {prefix})

`{prefix}send` - Send message multiple times
`{prefix}spm` - Start/stop continuous spam
`{prefix}stop` - Stop any active sending
`{prefix}restart` - Restart last command

Use `{prefix}help_bot [command]` for detailed usage"""

    elif category.lower() == "listening":
        return f"""**Keyword Listening** (Prefix: {prefix})

`{prefix}listento` - Start monitoring keywords
`{prefix}stoplisten` - Stop monitoring channel
`{prefix}editlisten` - Edit keyword settings

Use `{prefix}help_bot [command]` for detailed usage"""

    elif category.lower() == "system":
        return f"""**System Commands** (Prefix: >)

`>react` - Multi-bot reactions
`>stopall` - Emergency stop everything
`>showallbots` - List all active bots
`>generate` - Generate new bot account

Use `{prefix}help_bot [command]` for detailed usage"""

    elif category.lower() == "management":
        return f"""**Bot Management** (Prefix: >)

`>addbot` - Add new dynamic bot
`>removebot` - Remove dynamic bot
`>changeprefix` - Change bot prefix
`>adduser` - Add authorized user
`>removeuser` - Remove authorized user

Use `{prefix}help_bot [command]` for detailed usage"""

    return None


def get_command_help(command: str, prefix: str) -> tuple:
    """Get detailed help for a specific command - returns (part1, part2)"""
    
    cmd = command.lower().replace(prefix, "").replace(">", "")
    
    commands_help = {
        "send": (
            f"""**{prefix}send** - Send message multiple times

**Usage:** `{prefix}send [message] [delay] [amount]`
**Example:** `{prefix}send "Hello World" 1.0 5`""",
            f"""**Parameters:**
- message: Text to send (use quotes for spaces)
- delay: Seconds between messages (min {MIN_DELAY})
- amount: Number of repetitions (1-{MAX_AMOUNT})"""
        ),
        
        "spm": (
            f"""**{prefix}spm** - Continuous spam control

**Usage:** 
`{prefix}spm start [message] [delay]`
`{prefix}spm stop`""",
            f"""**Examples:**
`{prefix}spm start "Spam text" 0.5`
`{prefix}spm stop`

**Parameters:**
- message: Text to spam continuously
- delay: Seconds between messages (min {MIN_DELAY})"""
        ),
        
        "stop": (
            f"""**{prefix}stop** - Stop active commands

**Usage:** `{prefix}stop`""",
            f"""Stops any running send/spm commands and disables auto-restart for this bot."""
        ),
        
        "restart": (
            f"""**{prefix}restart** - Restart last command

**Usage:** `{prefix}restart`""",
            f"""Manually restarts the last command that was running on this bot."""
        ),
        
        "listento": (
            f"""**{prefix}listento** - Monitor keywords in channel

**Usage:** `{prefix}listento [case] [word] "keywords" [channel]`
**Example:** `{prefix}listento y n "hello,test" https://discord.com/channels/123/456`""",
            f"""**Parameters:**
- case: y/n for case-sensitive matching
- word: y/n for whole word matching only
- keywords: Comma-separated list in quotes
- channel: Discord channel URL or ID"""
        ),
        
        "stoplisten": (
            f"""**{prefix}stoplisten** - Stop monitoring channel

**Usage:** `{prefix}stoplisten [channel]`
**Example:** `{prefix}stoplisten https://discord.com/channels/123/456`""",
            f"""Stops keyword monitoring for the specified channel."""
        ),
        
        "editlisten": (
            f"""**{prefix}editlisten** - Edit keyword settings

**Usage:** `{prefix}editlisten [case] [word] "keywords" [channel]`
**Example:** `{prefix}editlisten n y "new,words" https://discord.com/channels/123/456`""",
            f"""Updates keyword list and matching settings for existing listener."""
        ),
        
        "react": (
            f"""**>react** - Multi-bot reactions

**Usage:** `>react [emojis] [count] [message] [delay]`
**Example:** `>react 😀,😎,🔥 5 https://discord.com/channels/123/456/789 0.5`""",
            f"""**Parameters:**
- emojis: Comma-separated emoji list
- count: Number of reactions (1-100)
- message: Discord message URL or ID
- delay: Seconds between reactions (optional, default: 1)"""
        ),
        
        "stopall": (
            f"""**>stopall** - Emergency stop

**Usage:** `>stopall`""",
            f"""Immediately stops ALL bots and commands across the entire system."""
        ),
        
        "showallbots": (
            f"""**>showallbots** - List active bots

**Usage:** `>showallbots`""",
            f"""Shows all currently running bots with their prefixes and types."""
        ),
        
        "addbot": (
            f"""**>addbot** - Add dynamic bot

**Usage:** `>addbot [prefix] [token]`
**Example:** `>addbot & your_bot_token_here`""",
            f"""Adds and starts a new bot with the specified prefix."""
        ),
        
        "removebot": (
            f"""**>removebot** - Remove dynamic bot

**Usage:** `>removebot [prefix]`
**Example:** `>removebot &`""",
            f"""Stops and removes a dynamic bot permanently."""
        ),
        
        "changeprefix": (
            f"""**>changeprefix** - Change bot prefix

**Usage:** `>changeprefix [old] [new]`
**Example:** `>changeprefix & %`""",
            f"""Changes prefix for dynamic bots (hardcoded bots need manual config)."""
        ),
        
        "adduser": (
            f"""**>adduser** - Authorize user for bot

**Usage:** `>adduser [user_ID] [prefix]`
**Example:** `>adduser 123456789012345678 &`""",
            f"""Grants user permission to use commands for specific bot."""
        ),
        
        "removeuser": (
            f"""**>removeuser** - Remove user authorization

**Usage:** `>removeuser [user_ID] [prefix]`
**Example:** `>removeuser 123456789012345678 &`""",
            f"""Revokes user permission for specific bot."""
        )
    }
    
    return commands_help.get(cmd, (f"Command `{cmd}` not found", ""))

    @bot.event
    async def on_command_error(ctx, error):
        """Handle command errors gracefully"""
        if isinstance(error, commands.CheckFailure):
            # Silently ignore authorization failures - no response to unauthorized users
            return
        elif isinstance(error, commands.CommandNotFound):
            # Only respond to command not found if user is authorized
            if check_user_authorization(ctx.author.id, prefix):
                try:
                    await ctx.author.send(
                        f"⚠️ Unknown command. Use `{prefix}help_bot` for available commands."
                    )
                except:
                    pass
        elif isinstance(error, commands.MissingRequiredArgument):
            # Only respond to missing arguments if user is authorized
            if check_user_authorization(ctx.author.id, prefix):
                try:
                    await ctx.author.send(
                        f"⚠️ Missing required arguments. Use `{prefix}help_bot` for command usage."
                    )
                except:
                    pass
        elif isinstance(error, commands.BadArgument):
            # Only respond to bad arguments if user is authorized
            if check_user_authorization(ctx.author.id, prefix):
                try:
                    await ctx.author.send(
                        f"⚠️ Invalid argument type. Use `{prefix}help_bot` for command usage."
                    )
                except:
                    pass
        else:
            # Only respond to general errors if user is authorized
            if check_user_authorization(ctx.author.id, prefix):
                try:
                    await ctx.author.send(f"⚠️ An error occurred: {error}")
                except:
                    pass
            print(f"Unhandled error in {bot_name}: {error}")

    return bot


async def handle_account_generation(message):
    """Handle account generation commands"""
    global generated_accounts, generation_tasks

    parts = message.content.split()
    if len(parts) < 3:
        try:
            await message.author.send(
                "⚠️ Usage: `>generate account [prefix]`\nExample: `>generate account &`"
            )
        except:
            pass
        return

    prefix = parts[2]
    user_id = message.author.id

    # Check if prefix is already in use
    existing_owner = get_prefix_owner(prefix)
    if existing_owner:
        try:
            await message.author.send(f"⚠️ Prefix '{prefix}' is already in use by: {existing_owner}")
        except:
            pass
        return

    # Check if generation is already in progress for this user
    if user_id in generation_tasks:
        try:
            await message.author.send(
                "⚠️ Account generation already in progress. Please wait...")
        except:
            pass
        return

    try:
        await message.author.send(
            f"🔄 Starting account generation for prefix '{prefix}'...\nThis may take 5-10 minutes. You'll be notified when complete."
        )
    except:
        pass

    # Start generation task
    task = asyncio.create_task(generate_and_deploy_account(prefix, user_id))
    generation_tasks[user_id] = task


async def generate_and_deploy_account(prefix, user_id):
    """Generate account and deploy new bot"""
    global generated_accounts, generation_tasks, BOT_CONFIGS, bots

    try:
        # Generate account
        result = await account_generator.generate_account(use_temp_email=True,
                                                          use_sms=True)

        if result['success']:
            token = result['token']
            username = result['username']

            # Store account data
            generated_accounts[prefix] = result

            # Add to bot configs
            token_env_name = f"TOKEN_{prefix.upper()}"
            BOT_CONFIGS[token_env_name] = prefix

            # Set environment variable (temporary for this session)
            os.environ[token_env_name] = token

            # Create and start new bot
            bot_name = f"Bot-{prefix}"
            bot = create_bot(prefix, bot_name)
            bots[prefix] = bot

            # Start bot
            asyncio.create_task(bot.start(token))

            # Save to file for persistence
            await save_generated_accounts()

            # Notify user
            try:
                user = None
                for bot in bots.values():
                    try:
                        user = await bot.fetch_user(user_id)
                        break
                    except:
                        continue

                if user:
                    await user.send(
                        f"✅ Account generated successfully!\n"
                        f"**Prefix:** {prefix}\n"
                        f"**Username:** {username}\n"
                        f"**Bot Status:** Online and ready\n"
                        f"You can now use `{prefix}send`, `{prefix}spm`, etc.")
            except:
                pass
        else:
            # Notify failure
            try:
                user = None
                for bot in bots.values():
                    try:
                        user = await bot.fetch_user(user_id)
                        break
                    except:
                        continue

                if user:
                    await user.send(
                        f"❌ Account generation failed: {result.get('error', 'Unknown error')}"
                    )
            except:
                pass

    except Exception as e:
        try:
            user = None
            for bot in bots.values():
                try:
                    user = await bot.fetch_user(user_id)
                    break
                except:
                    continue

            if user:
                await user.send(f"❌ Account generation error: {str(e)}")
        except:
            pass

    finally:
        # Clean up task
        generation_tasks.pop(user_id, None)


async def save_generated_accounts():
    """Save generated accounts to file"""
    try:
        async with aiofiles.open('generated_accounts.json', 'w') as f:
            await f.write(json.dumps(generated_accounts, indent=2))
    except Exception as e:
        print(f"Failed to save accounts: {e}")


async def load_generated_accounts():
    """Load generated accounts from file"""
    global generated_accounts
    try:
        async with aiofiles.open('generated_accounts.json', 'r') as f:
            content = await f.read()
            generated_accounts = json.loads(content)

            # Restore BOT_CONFIGS
            for prefix, account_data in generated_accounts.items():
                token_env_name = f"TOKEN_{prefix.upper()}"
                BOT_CONFIGS[token_env_name] = prefix
                # Note: Tokens would need to be restored from secure storage

    except FileNotFoundError:
        generated_accounts = {}
    except Exception as e:
        print(f"Failed to load accounts: {e}")
        generated_accounts = {}


async def run_multiple_bots():
    """Run multiple bot instances simultaneously"""
    # Load existing generated accounts, dynamic bots, and listening configs
    await load_generated_accounts()
    await load_dynamic_bots()
    await load_listening_configs()

    bot_tasks = []

    # Hardcoded tokens for local development
    HARDCODED_TOKENS = {
        "TOKEN": "",
        "TOKEN2": "",
        "TOKEN3": ""
    }

    # Start hardcoded bots
    for token_name, prefix in BOT_CONFIGS.items():
        # Use hardcoded token first, fallback to environment variable
        token = HARDCODED_TOKENS.get(token_name) or os.getenv(token_name)
        if token and token != f"YOUR_{token_name.split('TOKEN')[0]}DISCORD_TOKEN_HERE":
            bot = create_bot(prefix, f"Bot-{prefix}")
            bots[prefix] = bot  # Store by prefix for easier access

            # Create a task for this bot
            task = asyncio.create_task(bot.start(token))
            bot_tasks.append(task)
            print(f"🚀 Starting hardcoded bot with prefix '{prefix}' using {token_name}")
        else:
            print(
                f"⚠️ {token_name} not found or using placeholder, skipping bot with prefix '{prefix}'"
            )

    # Start dynamic bots
    for prefix, bot_data in dynamic_bots.items():
        token = bot_data['token']
        try:
            bot = create_bot(prefix, f"DynamicBot-{prefix}")
            bots[prefix] = bot

            # Create a task for this bot
            task = asyncio.create_task(bot.start(token))
            bot_tasks.append(task)
            print(f"🚀 Starting dynamic bot with prefix '{prefix}'")
        except Exception as e:
            print(f"❌ Failed to start dynamic bot '{prefix}': {e}")

    if not bot_tasks:
        print(
            "❌ No valid tokens found. Please add at least TOKEN to your secrets or use >addbot command."
        )
        return

    print(f"📊 Loaded {len(listening_configs)} keyword listeners from previous session")

    # Wait for all bots to finish (they should run indefinitely)
    try:
        await asyncio.gather(*bot_tasks, return_exceptions=True)
    except Exception as e:
        print(f"❌ Error running bots: {e}")


if __name__ == "__main__":
    # Start the Flask keep-alive server
    keep_alive()

    print("🤖 Discord Multi-Bot System with Enhanced Features Starting...")
    print("=" * 70)

    # Check API keys for account generation
    # Hardcoded API keys for local development (optional)
    HARDCODED_API_KEYS = {
        "SMS_ACTIVATE_API_KEY": "",  # Add your SMS-Activate API key here
        "CAPTCHA_API_KEY": ""  # Add your CAPTCHA API key here  
    }

    sms_key = HARDCODED_API_KEYS.get("SMS_ACTIVATE_API_KEY") or os.getenv(
        'SMS_ACTIVATE_API_KEY')
    captcha_key = HARDCODED_API_KEYS.get("CAPTCHA_API_KEY") or os.getenv(
        'CAPTCHA_API_KEY')

    print("Account Generation Services:")
    print(
        f"  SMS Service: {'✅ Ready' if sms_key else '❌ Missing SMS_ACTIVATE_API_KEY'}"
    )
    print(
        f"  CAPTCHA Service: {'✅ Ready' if captcha_key else '❌ Missing CAPTCHA_API_KEY'}"
    )
    print()

    print("Configured hardcoded bots:")
    for token_name, prefix in BOT_CONFIGS.items():
        token = os.getenv(token_name)
        status = "✅ Ready" if token else "❌ Missing"
        print(f"  {prefix} prefix - {token_name}: {status}")
    print()
    
    print("System Commands:")
    print("  >addbot [prefix] [token] - Add new Discord bot dynamically")
    print("  >removebot [prefix] - Remove dynamic bot")
    print("  >changeprefix [old_prefix] [new_prefix] - Change dynamic bot prefix")
    print("  >adduser [user_ID] [prefix] - Add authorized user for specific bot")
    print("  >removeuser [user_ID] [prefix] - Remove authorized user from specific bot")
    print("  >showallbots - Show all active bots")
    print("  >react [emojis] [num_reactions] [message_link] [delay] - Multi-bot reactions")
    print("  >generate account [prefix] - Generate new Discord account and bot")
    print("  >stopall - Emergency stop all bots")
    print()
    print("New Features:")
    print("  Keyword Listening - Monitor channels for specific keywords with customizable matching")
    print("  Multi-Bot Reactions - Distribute reactions across all active bots")
    print("  Dynamic Prefix Changes - Change bot prefixes without restart")
    print("  Enhanced Error Handling - Better resilience and auto-restart capabilities")
    print("=" * 70)

    try:
        asyncio.run(run_multiple_bots())
    except KeyboardInterrupt:
        print("\n🛑 Shutting down all bots...")
    except Exception as e:
        print(f"❌ Failed to start bots: {e}")
