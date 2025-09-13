import os
import discord
from discord.ext import commands
import asyncio
from keep_alive import keep_alive

import json
import aiofiles

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

# New global variables for enhanced features
bot_prefixes = {}  # Track prefix for each bot: {bot_user_id: prefix}
paused_spam = {}   # Track paused spam commands: {spam_key: (message, delay)}
current_spam_messages = {}  # Track current spam messages: {spam_key: message}
# ---------------------------


def store_last_command(prefix: str, user_id: int, command_type: str, channel_id: int = None, **kwargs):
    """Store the last executed command for auto-restart"""
    key = f"{prefix}_{user_id}"
    last_commands[key] = {
        'command_type': command_type,
        'timestamp': asyncio.get_event_loop().time(),
        'attempts': 0,
        'channel_id': channel_id,
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

    # Get the original channel for restart
    channel = None
    if command_data.get('channel_id'):
        try:
            channel = ctx.bot.get_channel(command_data['channel_id'])
            if not channel:
                channel = await ctx.bot.fetch_channel(command_data['channel_id'])
        except:
            channel = ctx.channel

    if not channel:
        channel = ctx.channel

    # Create a new context for the restart
    restart_ctx = type('RestartContext', (), {
        'send': channel.send,
        'channel': channel,
        'author': ctx.author,
        'bot': ctx.bot
    })()

    # Restart based on command type
    try:
        if command_data['command_type'] == 'send':
            # Recreate send command
            message = command_data['message']
            delay = command_data['delay']
            amount = command_data['amount']

            # Don't store again to avoid infinite loop
            await execute_send_command(restart_ctx,
                                       message,
                                       delay,
                                       amount,
                                       store_command=False)

        elif command_data['command_type'] == 'spm':
            # Recreate spm command
            message = command_data['message']
            delay = command_data['delay']

            # Don't store again to avoid infinite loop
            await execute_spm_command(restart_ctx,
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
                           channel_id=ctx.channel.id,
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
    spam_key = f"{prefix}_{user_id}"

    # Store current message for editing
    current_spam_messages[spam_key] = message

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

            # Check if spam is paused
            if spam_key in paused_spam:
                try:
                    await ctx.author.send(f"⏸️ Spam paused on {prefix} bot after {count} messages.")
                except:
                    pass
                
                # Wait for resume
                while spam_key in paused_spam and not emergency_stop:
                    await asyncio.sleep(1)
                
                if emergency_stop:
                    break
                    
                # Resume notification
                try:
                    await ctx.author.send(f"▶️ Spam resumed on {prefix} bot.")
                except:
                    pass

            # Get current message (might have been edited)
            current_message = current_spam_messages.get(spam_key, message)

            # ---- 503 resilience inside spam loop ----
            try:
                await ctx.send(current_message)
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
        # Clean up current message tracking
        current_spam_messages.pop(spam_key, None)
        raise
    except Exception as e:
        try:
            await ctx.author.send(f"⚠️ Spam error after {count} messages: {e}")
        except:
            pass
        # Clean up current message tracking
        current_spam_messages.pop(spam_key, None)
        raise
    finally:
        # Clean up current message tracking
        current_spam_messages.pop(spam_key, None)


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
                           channel_id=ctx.channel.id,
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


def get_bot_by_user_id(user_id_str):
    """Get bot by user ID or prefix"""
    # Try to find by user ID first
    for bot in bots.values():
        if str(bot.user.id) == user_id_str:
            return bot
    
    # Try to find by prefix
    if user_id_str in bots:
        return bots[user_id_str]
    
    return None


async def save_allowed_users():
    """Save allowed users to file"""
    try:
        async with aiofiles.open('allowed_users.json', 'w') as f:
            await f.write(json.dumps(ALLOWED_USERS, indent=2))
    except Exception as e:
        print(f"Failed to save allowed users: {e}")


async def load_allowed_users():
    """Load allowed users from file"""
    global ALLOWED_USERS
    try:
        async with aiofiles.open('allowed_users.json', 'r') as f:
            content = await f.read()
            ALLOWED_USERS = json.loads(content)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"Failed to load allowed users: {e}")


def create_bot(prefix: str, bot_name: str):
    """Create a bot instance with the given prefix"""
    # Using discord.py-self - no intents or self_bot needed
    bot = commands.Bot(command_prefix=prefix)

    @bot.event
    async def on_ready():
        print(f"✅ {bot_name} logged in as {bot.user} (ID: {bot.user.id})")
        print(
            f"Bot is ready and listening for commands with prefix '{prefix}'")
        print(f"Authorized users: {ALLOWED_USERS}")
        
        # Store bot prefix mapping
        bot_prefixes[bot.user.id] = prefix

    @bot.check
    async def is_allowed(ctx):
        """Global check to ensure only authorized users can use bot commands"""
        is_authorized = ctx.author.id in ALLOWED_USERS
        # Debug: Print user ID for troubleshooting
        if not is_authorized:
            print(f"❌ Unauthorized user tried command: {ctx.author.id} (not in {ALLOWED_USERS})")
        else:
            print(f"✅ Authorized user {ctx.author.id} using command: {ctx.command}")
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

        # Remove from paused if it was paused
        paused_spam.pop(spam_key, None)

        if user_id not in stop_flags and spam_key not in spam_tasks:
            try:
                await ctx.author.send(
                    f"ℹ️ No active message sending to stop on {prefix} bot.")
            except:
                pass

    @bot.command()
    async def spm(ctx, action: str, message: str = None, delay: float = 1.0):
        """
        Continuous spam command with start/stop/pause/resume functionality.

        Usage: {prefix}spm start [message] [delay]
               {prefix}spm stop
               {prefix}spm pause
               {prefix}spm resume

        Examples:
        {prefix}spm start "Hello" 1.0
        {prefix}spm pause
        {prefix}spm resume
        {prefix}spm stop
        """
        user_id = ctx.author.id
        spam_key = f"{prefix}_{user_id}"

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

            if len(message) > 2000:
                try:
                    await ctx.author.send(
                        "⚠️ Message is too long. Discord messages must be 2000 characters or less."
                    )
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

            # Remove from paused if it was paused
            paused_spam.pop(spam_key, None)

            if spam_key not in spam_tasks:
                try:
                    await ctx.author.send(
                        f"ℹ️ No active spam to stop on {prefix} bot.")
                except:
                    pass

        elif action.lower() == "pause":
            if spam_key in spam_tasks and spam_key not in paused_spam:
                # Get current command info
                last_command_key = f"{prefix}_{user_id}"
                if last_command_key in last_commands:
                    cmd_data = last_commands[last_command_key]
                    paused_spam[spam_key] = (cmd_data.get('message', ''), cmd_data.get('delay', 1.0))
                    try:
                        await ctx.author.send(f"⏸️ Spam paused on {prefix} bot. Use `{prefix}spm resume` to continue.")
                    except:
                        pass
                else:
                    try:
                        await ctx.author.send(f"⚠️ Could not pause spam - command info not found.")
                    except:
                        pass
            elif spam_key in paused_spam:
                try:
                    await ctx.author.send(f"ℹ️ Spam is already paused on {prefix} bot.")
                except:
                    pass
            else:
                try:
                    await ctx.author.send(f"ℹ️ No active spam to pause on {prefix} bot.")
                except:
                    pass

        elif action.lower() == "resume":
            if spam_key in paused_spam:
                paused_spam.pop(spam_key, None)
                try:
                    await ctx.author.send(f"▶️ Spam resumed on {prefix} bot.")
                except:
                    pass
            else:
                try:
                    await ctx.author.send(f"ℹ️ No paused spam to resume on {prefix} bot.")
                except:
                    pass

        else:
            try:
                await ctx.author.send(
                    f"⚠️ Invalid action. Use `start`, `stop`, `pause`, or `resume`.\nExample: `{prefix}spm start \"message\" 1.0`"
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
    async def help_bot(ctx):
        """Display help information about bot commands"""
        help_message = f"""🤖 **Discord Bot Help** (Prefix: {prefix})
Available commands for authorized users:

**`{prefix}send [message] [delay] [amount]`**
Send a message multiple times with delay
• message: Text to send (use quotes for spaces)
• delay: Seconds between messages (min {MIN_DELAY})
• amount: Number of repetitions (max {MAX_AMOUNT})
Example: `{prefix}send "Hello World" 1.0 3`

**`{prefix}spm start [message] [delay]`**
Start continuous spam (infinite messages until stopped)
• message: Text to spam (use quotes for spaces)
• delay: Seconds between messages (min {MIN_DELAY})
Example: `{prefix}spm start "Spam message" 0.5`

**`{prefix}spm pause/resume/stop`**
Control continuous spam
• pause: Temporarily pause spam
• resume: Resume paused spam
• stop: Stop spam completely

**`{prefix}stop`**
Stop any active message sending (works for both send and spm)
Also disables auto-restart for this bot

**`{prefix}restart`**
Manually restart the last command that was running

**Global Commands (work with any bot):**
**`>stopall`**
🚨 EMERGENCY STOP - Immediately stops ALL bots and commands

**`>addbot [token]`**
Add a new bot with automatic prefix assignment

**`>edit [bot_user_id/prefix] "new message"`**
Edit the current spam message of any bot
Examples: `>edit $ "new message"` or `>edit 123456789 "new message"`

**`>allow [user_id]`**
Add user to allowed users list

**`>revoke [user_id]`**
Remove user from allowed users list

**`>retry [bot_user_id/prefix]`**
Retry the last command on specified bot in original channel

**`>changeprefix [bot_user_id] [new_prefix]`**
Change prefix of any bot

**Auto-Restart Features**
• Commands automatically restart after errors (max {MAX_RESTART_ATTEMPTS} attempts)
• {RESTART_DELAY} second delay before restart attempts
• Manual restart available with `{prefix}restart`
• Auto-restart disabled when using stop commands

**Safety Features**
• Minimum delay: {MIN_DELAY} seconds
• User authorization required
• Individual stop controls per user
• Emergency stop for all bots
• Automatic command cleanup

Bot is running 24/7 on Replit with keep-alive monitoring"""

        await ctx.send(help_message)

    @bot.event
    async def on_message(message):
        """Handle global commands and regular commands"""
        global emergency_stop, ALLOWED_USERS

        # Ignore bot messages
        if message.author.bot:
            return

        # Check for emergency stopall command
        if message.content == ">stopall" and message.author.id in ALLOWED_USERS:
            emergency_stop = True

            # Cancel all active spam tasks
            for spam_key, task in list(spam_tasks.items()):
                task.cancel()
                spam_tasks.pop(spam_key, None)

            # Set all stop flags
            for user_id in list(stop_flags.keys()):
                stop_flags[user_id] = True

            # Clear all last commands to prevent auto-restart
            last_commands.clear()
            
            # Clear paused spam
            paused_spam.clear()

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

        # Check for addbot command
        if message.content.startswith(">addbot ") and message.author.id in ALLOWED_USERS:
            parts = message.content.split(" ", 1)
            if len(parts) != 2:
                try:
                    await message.author.send("⚠️ Usage: `>addbot [token]`")
                except:
                    pass
                return

            token = parts[1].strip()
            
            # Find an available prefix
            used_prefixes = set(BOT_CONFIGS.values())
            available_prefixes = ["#", "&", "%", "@", "*", "+", "=", "-", "_", "~", "`", "^"]
            
            new_prefix = None
            for prefix_option in available_prefixes:
                if prefix_option not in used_prefixes:
                    new_prefix = prefix_option
                    break
            
            if not new_prefix:
                try:
                    await message.author.send("❌ No available prefixes. All prefixes are in use.")
                except:
                    pass
                return

            try:
                # Create and start new bot
                bot_name = f"Bot-{new_prefix}"
                new_bot = create_bot(new_prefix, bot_name)
                
                # Add to configurations
                token_name = f"TOKEN_{new_prefix.replace('#', 'HASH').replace('&', 'AMP').replace('%', 'PERCENT').replace('@', 'AT').replace('*', 'STAR').replace('+', 'PLUS').replace('=', 'EQUALS').replace('-', 'DASH').replace('_', 'UNDER').replace('~', 'TILDE').replace('`', 'TICK').replace('^', 'CARET')}"
                BOT_CONFIGS[token_name] = new_prefix
                bots[new_prefix] = new_bot
                
                # Start the bot
                asyncio.create_task(new_bot.start(token))
                
                try:
                    await message.author.send(f"✅ Bot added successfully with prefix `{new_prefix}`! Bot will be online shortly.")
                except:
                    pass
                
            except Exception as e:
                try:
                    await message.author.send(f"❌ Failed to add bot: {str(e)}")
                except:
                    pass
            return

        # Check for edit command
        if message.content.startswith(">edit ") and message.author.id in ALLOWED_USERS:
            parts = message.content.split(" ", 2)
            if len(parts) != 3:
                try:
                    await message.author.send("⚠️ Usage: `>edit [bot_user_id/prefix] \"new message\"`")
                except:
                    pass
