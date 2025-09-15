# System commands (prefix: >)
       if content.startswith(">") and user_id in ALLOWED_USERS:
            # Handle system commands here directly instead of separate function
            await handle_system_commands_inline(message)
            return
        
        # Process normal commands for this bot
        await bot.process_commands(message)
    
    return bot


# NEW: System commands handler function
async def handle_system_commands_inline(message):
    """Handle system commands that work across all bots"""
    global emergency_stop
    
    content = message.content
    user_id = message.author.id
    
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

    # NEW: Change prefix command
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
        
        # Check if old bot exists and is dynamic
        if old_prefix not in dynamic_bots:
            try:
                await message.author.send(f"❌ No dynamic bot found with prefix '{old_prefix}'. Only dynamic bots can have their prefix changed.")
            except:
                pass
            return
        
        # Check if new prefix is already in use
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
            # Get bot data
            bot_data = dynamic_bots[old_prefix]
            
            # Stop all tasks for the old bot
            tasks_stopped = await stop_bot_tasks(old_prefix)
            
            # Close old bot
            if old_prefix in bots:
                try:
                    await bots[old_prefix].close()
                except:
                    pass
                bots.pop(old_prefix, None)
            
            # Update data with new prefix
            bot_data['prefix'] = new_prefix
            bot_data['updated_at'] = datetime.now().isoformat()
            
            # Move to new prefix key
            dynamic_bots[new_prefix] = bot_data
            dynamic_bots.pop(old_prefix, None)
            await save_dynamic_bots()
            
            # Create new bot with new prefix
            bot_name = f"DynamicBot-{new_prefix}"
            new_bot = create_bot(new_prefix, bot_name)
            bots[new_prefix] = new_bot
            
            # Start bot with new prefix
            asyncio.create_task(new_bot.start(bot_data['token']))
            
            try:
                await message.author.send(
                    f"✅ Bot prefix changed from '{old_prefix}' to '{new_prefix}' successfully!\n"
                    f"Stopped {tasks_stopped} tasks and restarted bot with new prefix."
                )
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

    # Account generation command
    elif content.startswith(">generate account") and user_id in ALLOWED_USERS:
        await handle_account_generation(message)
        return


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
    # Load existing configurations
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
    
    print("New Features Added:")
    print("  🔔 Keyword Listening - Monitor channels for specific keywords")
    print("  ⚡ Multi-Bot Reactions - Use all bots to add reactions")
    print("  🔧 Dynamic Prefix Changing - Change prefixes for dynamic bots")
    print()
    
    print("System Commands:")
    print("  >addbot [prefix] [token] - Add new Discord bot dynamically")
    print("  >removebot [prefix] - Remove dynamic bot")
    print("  >changeprefix [old_prefix] [new_prefix] - Change dynamic bot prefix")
    print("  >adduser [user_ID] [prefix] - Add authorized user for specific bot")
    print("  >removeuser [user_ID] [prefix] - Remove authorized user from specific bot")
    print("  >showallbots - Show all active bots")
    print("  >generate account [prefix] - Generate new Discord account and bot")
    print("  >stopall - Emergency stop all bots")
    print("=" * 70)

    try:
        asyncio.run(run_multiple_bots())
    except KeyboardInterrupt:
        print("\n🛑 Shutting down all bots...")
    except Exception as e:
        print(f"❌ Failed to start bots: {e}")import os
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

# NEW: Keyword listening system
listening_configs = {}  # Store listening configurations
LISTENING_CONFIGS_FILE = 'listening_configs.json'
listening_tasks = {}  # Track active listening tasks

# NEW: Multi-bot reaction system
reaction_tasks = {}  # Track ongoing reaction tasks
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


# NEW: Keyword listening utility functions
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


def parse_discord_link(link: str):
    """Parse Discord message/channel link and extract IDs"""
    try:
        # Remove discord.com part if present
        if 'discord.com' in link:
            link = link.split('discord.com/channels/')[1]
        elif link.startswith('https://'):
            return None
            
        parts = link.strip('/').split('/')
        
        if len(parts) == 2:
            # Channel link: guild_id/channel_id
            guild_id, channel_id = parts
            return {
                'guild_id': int(guild_id),
                'channel_id': int(channel_id),
                'message_id': None
            }
        elif len(parts) == 3:
            # Message link: guild_id/channel_id/message_id
            guild_id, channel_id, message_id = parts
            return {
                'guild_id': int(guild_id),
                'channel_id': int(channel_id),
                'message_id': int(message_id)
            }
        else:
            return None
    except (ValueError, IndexError):
        return None


def check_keywords_match(message_content: str, keywords: list, case_sensitive: bool, word_match: bool):
    """Check if message contains any of the specified keywords"""
    content = message_content if case_sensitive else message_content.lower()
    
    for keyword in keywords:
        check_keyword = keyword if case_sensitive else keyword.lower()
        
        if word_match:
            # Use word boundaries for exact word matching
            pattern = r'\b' + re.escape(check_keyword) + r'\b'
            if re.search(pattern, content):
                return True
        else:
            # Simple substring search
            if check_keyword in content:
                return True
    
    return False


async def handle_keyword_match(message, keywords_matched, listener_data):
    """Handle when a message matches keywords"""
    try:
        user_id = listener_data['user_id']
        
        # Get user object from any available bot
        user = None
        for bot in bots.values():
            try:
                user = await bot.fetch_user(user_id)
                break
            except:
                continue
        
        if not user:
            return
        
        # Create message link
        message_link = f"https://discord.com/channels/{message.guild.id if message.guild else '@me'}/{message.channel.id}/{message.id}"
        
        # Format DM content
        dm_content = f"🔔 **Keyword Alert**\n"
        dm_content += f"**Keywords matched:** {', '.join(keywords_matched)}\n"
        dm_content += f"**Author:** {message.author} ({message.author.id})\n"
        dm_content += f"**Channel:** {message.channel.name if hasattr(message.channel, 'name') else 'DM'}\n"
        if message.guild:
            dm_content += f"**Server:** {message.guild.name}\n"
        dm_content += f"**Message:** {message.content[:500]}{'...' if len(message.content) > 500 else ''}\n"
        dm_content += f"**Link:** {message_link}"
        
        await user.send(dm_content)
        
    except Exception as e:
        print(f"Error handling keyword match: {e}")


def get_listening_key(channel_id: int, user_id: int):
    """Generate unique key for listening configuration"""
    return f"{channel_id}_{user_id}"


# NEW: Multi-bot reaction utilities
async def get_available_bots_for_guild(guild_id: int):
    """Get list of bots that can access the specified guild"""
    available_bots = []
    for prefix, bot in bots.items():
        try:
            guild = bot.get_guild(guild_id)
            if guild:
                available_bots.append(bot)
        except:
            continue
    return available_bots


async def execute_multi_bot_reactions(message_link: str, emojis: list, reaction_count: int, delay: float, user_id: int):
    """Execute reactions using multiple bots"""
    try:
        # Parse message link
        link_data = parse_discord_link(message_link)
        if not link_data or not link_data['message_id']:
            return False, "Invalid message link provided"
        
        guild_id = link_data['guild_id']
        channel_id = link_data['channel_id']
        message_id = link_data['message_id']
        
        # Get available bots for this guild
        available_bots = await get_available_bots_for_guild(guild_id)
        
        if not available_bots:
            return False, "No bots available for this server"
        
        # Get the target message
        target_message = None
        for bot in available_bots:
            try:
                channel = bot.get_channel(channel_id)
                if channel:
                    target_message = await channel.fetch_message(message_id)
                    break
            except:
                continue
        
        if not target_message:
            return False, "Could not access target message"
        
        # Distribute reactions across available bots
        reactions_added = 0
        total_reactions_needed = len(emojis) * reaction_count
        
        for emoji in emojis:
            for _ in range(reaction_count):
                if reactions_added >= total_reactions_needed:
                    break
                
                # Select bot (cycle through available bots)
                bot = available_bots[reactions_added % len(available_bots)]
                
                try:
                    # Get the channel from this specific bot
                    channel = bot.get_channel(channel_id)
                    if channel:
                        message = await channel.fetch_message(message_id)
                        await message.add_reaction(emoji)
                        reactions_added += 1
                        
                        # Add delay between reactions
                        if delay > 0 and reactions_added < total_reactions_needed:
                            await asyncio.sleep(delay)
                
                except discord.HTTPException as e:
                    # Skip this reaction if it fails
                    continue
                except Exception as e:
                    continue
        
        return True, f"Successfully added {reactions_added}/{total_reactions_needed} reactions"
        
    except Exception as e:
        return False, f"Error executing reactions: {str(e)}"


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
    
    # NEW: Stop listening tasks for this bot
    listening_keys_to_remove = []
    for listening_key in listening_tasks:
        if listening_key.endswith(f"_{prefix}"):
            listening_tasks[listening_key].cancel()
            listening_keys_to_remove.append(listening_key)
            tasks_stopped += 1
    
    for key in listening_keys_to_remove:
        listening_tasks.pop(key, None)
    
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
        """Handle both keyword listening and regular command processing"""
        # Ignore messages from bots
        if message.author.bot:
            return

        # Check for keyword matches in this channel
        channel_id = message.channel.id
        matched_listeners = []
        
        for listener_key, listener_data in listening_configs.items():
            if listener_data['channel_id'] == channel_id:
                keywords = listener_data['keywords']
                case_sensitive = listener_data['case_sensitive']
                word_match = listener_data['word_match']
                
                if check_keywords_match(message.content, keywords, case_sensitive, word_match):
                    # Find which keywords matched
                    content = message.content if case_sensitive else message.content.lower()
                    keywords_matched = []
                    
                    for keyword in keywords:
                        check_keyword = keyword if case_sensitive else keyword.lower()
                        if word_match:
                            pattern = r'\b' + re.escape(check_keyword) + r'\b'
                            if re.search(pattern, content):
                                keywords_matched.append(keyword)
                        else:
                            if check_keyword in content:
                                keywords_matched.append(keyword)
                    
                    if keywords_matched:
                        # Handle the match asynchronously
                        asyncio.create_task(handle_keyword_match(message, keywords_matched, listener_data))

        # Process regular commands for system commands and this bot's commands
        user_id = message.author.id
        content = message.content

        # System commands (prefix: >)
        if content.startswith(">") and user_id in ALLOWED_USERS:
            await handle_system_commands(message)
            return
        
        # Process normal commands for this bot
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

    # NEW: Keyword listening commands
    @bot.command()
    async def listento(ctx, case_sensitive: str, word_match: str, keywords: str, channel_link: str):
        """
        Start listening for keywords in a channel and DM when found.
        
        Usage: {prefix}listento [y/n] [y/n] "keyword1, keyword2, etc.." [channel_link]
        
        Parameters:
        - case_sensitive: y for case-sensitive, n for case-insensitive
        - word_match: y for whole words only, n for partial matches
        - keywords: Comma-separated list of keywords (use quotes)
        - channel_link: Discord channel or message link
        """
        try:
            # Delete the command message
            try:
                await ctx.message.delete()
            except:
                pass
            
            # Validate parameters
            case_sens = case_sensitive.lower() == 'y'
            word_match_bool = word_match.lower() == 'y'
            
            if case_sensitive.lower() not in ['y', 'n'] or word_match.lower() not in ['y', 'n']:
                try:
                    await ctx.author.send("⚠️ Case sensitive and word match parameters must be 'y' or 'n'")
                except:
                    pass
                return
            
            # Parse channel link
            link_data = parse_discord_link(channel_link)
            if not link_data:
                try:
                    await ctx.author.send("⚠️ Invalid channel link provided")
                except:
                    pass
                return
            
            channel_id = link_data['channel_id']
            
            # Verify bot can access the channel
            channel = bot.get_channel(channel_id)
            if not channel:
                try:
                    await ctx.author.send("⚠️ Bot cannot access the specified channel")
                except:
                    pass
                return
            
            # Parse keywords
            keyword_list = [kw.strip() for kw in keywords.split(',') if kw.strip()]
            if not keyword_list:
                try:
                    await ctx.author.send("⚠️ No valid keywords provided")
                except:
                    pass
                return
            
            # Create listener configuration
            listener_key = get_listening_key(channel_id, ctx.author.id)
            listener_config = {
                'user_id': ctx.author.id,
                'channel_id': channel_id,
                'guild_id': link_data['guild_id'],
                'keywords': keyword_list,
                'case_sensitive': case_sens,
                'word_match': word_match_bool,
                'created_at': datetime.now().isoformat(),
                'bot_prefix': prefix
            }
            
            # Store configuration
            listening_configs[listener_key] = listener_config
            await save_listening_configs()
            
            # Send confirmation
            case_text = "case-sensitive" if case_sens else "case-insensitive"
            match_text = "whole words" if word_match_bool else "partial matches"
            
            try:
                await ctx.author.send(
                    f"✅ Now listening in {channel.name} for keywords: {', '.join(keyword_list)}\n"
                    f"Settings: {case_text}, {match_text}\n"
                    f"Use `{prefix}stoplisten {channel_link}` to stop listening."
                )
            except:
                pass
                
        except Exception as e:
            try:
                await ctx.author.send(f"⚠️ Error setting up keyword listening: {e}")
            except:
                pass

    @bot.command()
    async def stoplisten(ctx, channel_link: str):
        """
        Stop listening for keywords in a channel.
        
        Usage: {prefix}stoplisten [channel_link]
        """
        try:
            # Delete the command message
            try:
                await ctx.message.delete()
            except:
                pass
            
            # Parse channel link
            link_data = parse_discord_link(channel_link)
            if not link_data:
                try:
                    await ctx.author.send("⚠️ Invalid channel link provided")
                except:
                    pass
                return
            
            channel_id = link_data['channel_id']
            listener_key = get_listening_key(channel_id, ctx.author.id)
            
            if listener_key not in listening_configs:
                try:
                    await ctx.author.send("⚠️ No active listening found for this channel")
                except:
                    pass
                return
            
            # Remove configuration
            removed_config = listening_configs.pop(listener_key, None)
            await save_listening_configs()
            
            # Get channel name for confirmation
            channel_name = "Unknown Channel"
            try:
                channel = bot.get_channel(channel_id)
                if channel:
                    channel_name = channel.name
            except:
                pass
            
            try:
                await ctx.author.send(f"🛑 Stopped listening for keywords in {channel_name}")
            except:
                pass
                
        except Exception as e:
            try:
                await ctx.author.send(f"⚠️ Error stopping keyword listening: {e}")
            except:
                pass

    @bot.command()
    async def editlisten(ctx, case_sensitive: str, word_match: str, keywords: str, channel_link: str):
        """
        Edit existing keyword listening configuration.
        
        Usage: {prefix}editlisten [y/n] [y/n] "keyword1, keyword2, etc.." [channel_link]
        """

