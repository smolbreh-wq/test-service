import os
import discord
from discord.ext import commands
import asyncio
from keep_alive import keep_alive
import json
import aiofiles

# ---------- CONFIG ----------
BOT_CONFIGS = {
    "TOKEN": "$",
    "TOKEN2": "!",
    "TOKEN3": "?",
}

ALLOWED_USERS = [
    1096838620712804405,
    1348330851263315968,
    1414215242388344994
]

MIN_DELAY = 0.5
MAX_AMOUNT = 20

# Global state management
spam_tasks = {}
stop_flags = {}
bots = {}
emergency_stop = False
last_commands = {}
spam_states = {}  # Store current spam message for editing
bot_tokens = {}   # Store bot tokens for management

MAX_RESTART_ATTEMPTS = 3
RESTART_DELAY = 5

# ---------------------------

async def save_user_data():
    """Save allowed users to file"""
    try:
        async with aiofiles.open('user_data.json', 'w') as f:
            await f.write(json.dumps({'allowed_users': ALLOWED_USERS}, indent=2))
    except Exception as e:
        print(f"Failed to save user data: {e}")

async def load_user_data():
    """Load allowed users from file"""
    global ALLOWED_USERS
    try:
        async with aiofiles.open('user_data.json', 'r') as f:
            content = await f.read()
            data = json.loads(content)
            ALLOWED_USERS.extend([uid for uid in data.get('allowed_users', []) if uid not in ALLOWED_USERS])
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"Failed to load user data: {e}")

def is_authorized(user_id: int) -> bool:
    """Check if user is authorized"""
    return user_id in ALLOWED_USERS

def get_bot_by_prefix(prefix: str):
    """Get bot instance by prefix"""
    return bots.get(prefix)

def store_last_command(prefix: str, user_id: int, command_type: str, **kwargs):
    """Store the last executed command for auto-restart"""
    key = f"{prefix}_{user_id}"
    last_commands[key] = {
        'command_type': command_type,
        'timestamp': asyncio.get_event_loop().time(),
        'attempts': 0,
        'prefix': prefix,
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

    if command_data['attempts'] > MAX_RESTART_ATTEMPTS:
        try:
            await ctx.author.send(
                f"❌ Max restart attempts ({MAX_RESTART_ATTEMPTS}) reached for {prefix} bot."
            )
        except:
            pass
        last_commands.pop(key, None)
        return False

    try:
        await ctx.author.send(
            f"🔄 Auto-restarting {prefix} bot (attempt {command_data['attempts']}/{MAX_RESTART_ATTEMPTS})"
            f"{f' - Error: {error_msg}' if error_msg else ''}"
        )
    except:
        pass

    await asyncio.sleep(RESTART_DELAY)

    try:
        if command_data['command_type'] == 'send':
            await execute_send_command(ctx, command_data['message'], 
                                     command_data['delay'], command_data['amount'], 
                                     store_command=False)
        elif command_data['command_type'] == 'spm':
            await execute_spm_command(ctx, prefix, command_data['message'], 
                                    command_data['delay'], store_command=False)
        return True
    except Exception as e:
        try:
            await ctx.author.send(f"❌ Failed to restart {prefix} bot: {e}")
        except:
            pass
        return False

async def execute_send_command(ctx, message: str, delay: float, amount: int, store_command: bool = True):
    """Execute send command with error handling and restart"""
    prefix = ctx.bot.command_prefix
    user_id = ctx.author.id

    if store_command:
        store_last_command(prefix, user_id, 'send', message=message, delay=delay, amount=amount)

    stop_flags[user_id] = False

    for i in range(amount):
        if stop_flags.get(user_id, False) or emergency_stop:
            try:
                status = "Emergency stop" if emergency_stop else "Manual stop"
                await ctx.author.send(f"🛑 {status} - Sent {i} messages")
            except:
                pass
            break

        try:
            await ctx.send(message)
            if i < amount - 1:
                await asyncio.sleep(delay)
        except (discord.errors.DiscordServerError, discord.HTTPException) as e:
            if getattr(e, "status", None) == 503:
                await asyncio.sleep(3)
                continue
            try:
                await ctx.author.send(f"⚠️ Error: {e}")
            except:
                pass
            if await restart_last_command(ctx, prefix, str(e)):
                return
            break
        except Exception as e:
            try:
                await ctx.author.send(f"⚠️ Unexpected error: {e}")
            except:
                pass
            if await restart_last_command(ctx, prefix, str(e)):
                return
            break

    stop_flags.pop(user_id, None)
    key = f"{prefix}_{user_id}"
    if key in last_commands and last_commands[key]['command_type'] == 'send':
        last_commands.pop(key, None)

async def spam_loop_with_restart(ctx, message: str, delay: float, prefix: str):
    """Continuous spam loop with restart capability"""
    global emergency_stop
    count = 0
    user_id = ctx.author.id
    spam_key = f"{prefix}_{user_id}"

    try:
        while spam_key in spam_tasks and not emergency_stop:
            # Check for message updates
            if spam_key in spam_states:
                current_message = spam_states[spam_key].get('message', message)
                if current_message != message:
                    message = current_message
                    try:
                        await ctx.author.send(f"📝 Updated spam message to: '{message}'")
                    except:
                        pass

            try:
                await ctx.send(message)
                count += 1
            except (discord.errors.DiscordServerError, discord.HTTPException) as e:
                if getattr(e, "status", None) == 503:
                    await asyncio.sleep(3)
                    continue
                if await restart_last_command(ctx, prefix, str(e)):
                    return
                raise
            except Exception as e:
                if await restart_last_command(ctx, prefix, str(e)):
                    return
                raise

            await asyncio.sleep(delay)
    except asyncio.CancelledError:
        try:
            await ctx.author.send(f"🛑 Spam stopped after {count} messages")
        except:
            pass
        raise
    except Exception as e:
        try:
            await ctx.author.send(f"⚠️ Spam error after {count} messages: {e}")
        except:
            pass
        raise
    finally:
        # Clean up
        spam_states.pop(spam_key, None)
        key = f"{prefix}_{user_id}"
        if key in last_commands and last_commands[key]['command_type'] == 'spm':
            last_commands.pop(key, None)

async def execute_spm_command(ctx, prefix: str, message: str, delay: float, store_command: bool = True):
    """Execute spam command with state management"""
    user_id = ctx.author.id
    spam_key = f"{prefix}_{user_id}"

    if store_command:
        store_last_command(prefix, user_id, 'spm', message=message, delay=delay)

    # Stop existing spam
    if spam_key in spam_tasks:
        spam_tasks[spam_key].cancel()
        spam_tasks.pop(spam_key, None)

    # Store spam state for editing
    spam_states[spam_key] = {'message': message, 'delay': delay, 'active': True}

    if store_command:
        try:
            await ctx.author.send(f"🚀 Starting spam on {prefix} bot: '{message}' ({delay}s delay)")
        except:
            pass

    task = asyncio.create_task(spam_loop_with_restart(ctx, message, delay, prefix))
    spam_tasks[spam_key] = task

    try:
        await task
    except asyncio.CancelledError:
        pass
    finally:
        spam_tasks.pop(spam_key, None)
        spam_states.pop(spam_key, None)

def create_bot(prefix: str, bot_name: str):
    """Create optimized bot instance"""
    bot = commands.Bot(command_prefix=prefix)

    @bot.event
    async def on_ready():
        print(f"✅ {bot_name} online as {bot.user}")

    @bot.check
    async def is_allowed(ctx):
        return is_authorized(ctx.author.id)

    @bot.command()
    async def send(ctx, message: str, delay: float, amount: int):
        """Send message multiple times: {prefix}send "message" delay amount"""
        if delay < MIN_DELAY:
            return await ctx.author.send(f"⚠️ Min delay: {MIN_DELAY}s")
        if not (1 <= amount <= MAX_AMOUNT):
            return await ctx.author.send(f"⚠️ Amount: 1-{MAX_AMOUNT}")
        if len(message) > 2000:
            return await ctx.author.send("⚠️ Message too long (2000 char limit)")

        try:
            await ctx.message.delete()
        except:
            pass
        
        await execute_send_command(ctx, message, delay, amount)

    @bot.command()
    async def spm(ctx, action: str, message: str = None, delay: float = 1.0):
        """Spam control: {prefix}spm start/stop "message" delay"""
        user_id = ctx.author.id
        spam_key = f"{prefix}_{user_id}"

        if action.lower() == "start":
            if not message:
                return await ctx.author.send("⚠️ Need message to spam")
            if delay < MIN_DELAY:
                return await ctx.author.send(f"⚠️ Min delay: {MIN_DELAY}s")
            
            try:
                await ctx.message.delete()
            except:
                pass
            
            await execute_spm_command(ctx, prefix, message, delay)

        elif action.lower() == "stop":
            if spam_key in spam_tasks:
                spam_tasks[spam_key].cancel()
                spam_tasks.pop(spam_key, None)
                spam_states.pop(spam_key, None)
                try:
                    await ctx.author.send(f"🛑 Spam stopped on {prefix} bot")
                except:
                    pass
            
            # Clear restart data
            last_key = f"{prefix}_{user_id}"
            last_commands.pop(last_key, None)

        else:
            await ctx.author.send("⚠️ Use 'start' or 'stop'")

    @bot.command()
    async def stop(ctx):
        """Stop all activities for this user on this bot"""
        user_id = ctx.author.id
        spam_key = f"{prefix}_{user_id}"

        # Stop send command
        if user_id in stop_flags:
            stop_flags[user_id] = True

        # Stop spam
        if spam_key in spam_tasks:
            spam_tasks[spam_key].cancel()
            spam_tasks.pop(spam_key, None)
            spam_states.pop(spam_key, None)

        # Clear restart data
        last_commands.pop(f"{prefix}_{user_id}", None)

        try:
            await ctx.author.send(f"🛑 All activities stopped on {prefix} bot")
        except:
            pass

    @bot.command()
    async def restart(ctx):
        """Manually restart last command"""
        user_id = ctx.author.id
        key = f"{prefix}_{user_id}"

        if key not in last_commands:
            return await ctx.author.send("ℹ️ No command to restart")

        last_commands[key]['attempts'] = 0
        await ctx.author.send(f"🔄 Restarting last command...")
        await restart_last_command(ctx, prefix)

    @bot.command()
    async def help_bot(ctx):
        """Show help for this bot"""
        help_text = f"""🤖 **Bot Help** (Prefix: {prefix})

**Basic Commands:**
• `{prefix}send "message" delay amount` - Send message multiple times
• `{prefix}start "message" delay` - Start spam (or just `{prefix}start` for last/edited)
• `{prefix}spm start "message" delay` - Start spam (same as start)
• `{prefix}spm stop` - Stop spam
• `{prefix}stop` - Stop all activities
• `{prefix}restart` - Restart last command

**Management Commands:**
• `>addbot token prefix` - Add new bot
• `>edit prefix "new message"` - Edit active spam message
• `>allow userid` - Add authorized user
• `>revoke userid` - Remove authorized user
• `>stopall` - Emergency stop all bots

**Limits:** Min delay {MIN_DELAY}s, Max amount {MAX_AMOUNT}"""
        
        await ctx.send(help_text)

    @bot.event
    async def on_message(message):
        """Handle special commands and regular processing"""
        global emergency_stop

        if not is_authorized(message.author.id):
            return

        content = message.content

        # Emergency stop all
        if content == ">stopall":
            emergency_stop = True
            
            # Cancel all tasks
            for task in list(spam_tasks.values()):
                task.cancel()
            spam_tasks.clear()
            spam_states.clear()
            
            # Set stop flags
            for user_id in list(stop_flags.keys()):
                stop_flags[user_id] = True
                
            last_commands.clear()
            
            try:
                await message.author.send("🚨 EMERGENCY STOP - All bots stopped!")
            except:
                pass
            
            await asyncio.sleep(1)
            emergency_stop = False
            return

        # Add new bot
        if content.startswith(">addbot "):
            parts = content.split(maxsplit=2)
            if len(parts) != 3:
                try:
                    await message.author.send("Usage: >addbot token prefix")
                except:
                    pass
                return
            
            token, new_prefix = parts[1], parts[2]
            
            if new_prefix in bots:
                try:
                    await message.author.send(f"❌ Prefix '{new_prefix}' already exists")
                except:
                    pass
                return
            
            try:
                new_bot = create_bot(new_prefix, f"Bot-{new_prefix}")
                bots[new_prefix] = new_bot
                bot_tokens[new_prefix] = token
                
                # Start the new bot
                asyncio.create_task(new_bot.start(token))
                
                await message.author.send(f"✅ Bot added with prefix '{new_prefix}' - Starting...")
            except Exception as e:
                await message.author.send(f"❌ Failed to add bot: {e}")
            return

        # Edit spam message
        if content.startswith(">edit "):
            parts = content.split(maxsplit=2)
            if len(parts) != 3:
                try:
                    await message.author.send("Usage: >edit prefix \"new message\"")
                except:
                    pass
                return
            
            target_prefix, new_message = parts[1], parts[2].strip('"')
            user_id = message.author.id
            spam_key = f"{target_prefix}_{user_id}"
            
            if spam_key in spam_states:
                spam_states[spam_key]['message'] = new_message
                try:
                    await message.author.send(f"✅ Updated {target_prefix} bot spam message")
                except:
                    pass
            else:
                try:
                    await message.author.send(f"❌ No active spam on {target_prefix} bot")
                except:
                    pass
            return

        # User management
        if content.startswith(">allow "):
            try:
                user_id = int(content.split()[1])
                if user_id not in ALLOWED_USERS:
                    ALLOWED_USERS.append(user_id)
                    await save_user_data()
                    await message.author.send(f"✅ Added user {user_id}")
                else:
                    await message.author.send(f"ℹ️ User {user_id} already authorized")
            except (ValueError, IndexError):
                await message.author.send("Usage: >allow userid")
            return

        if content.startswith(">revoke "):
            try:
                user_id = int(content.split()[1])
                if user_id in ALLOWED_USERS:
                    ALLOWED_USERS.remove(user_id)
                    await save_user_data()
                    await message.author.send(f"✅ Removed user {user_id}")
                else:
                    await message.author.send(f"ℹ️ User {user_id} not in authorized list")
            except (ValueError, IndexError):
                await message.author.send("Usage: >revoke userid")
            return

        await bot.process_commands(message)

    @bot.event
    async def on_command_error(ctx, error):
        """Simplified error handling"""
        if isinstance(error, commands.CheckFailure):
            return
        
        if is_authorized(ctx.author.id):
            error_msg = "Unknown command" if isinstance(error, commands.CommandNotFound) else str(error)
            try:
                await ctx.author.send(f"⚠️ {error_msg}")
            except:
                pass

    return bot

async def run_multiple_bots():
    """Run all configured bots"""
    await load_user_data()
    
    # Hardcoded tokens
    HARDCODED_TOKENS = {
        "TOKEN": "",
        "TOKEN2": "",
        "TOKEN3": ""
    }

    bot_tasks = []
    
    for token_name, prefix in BOT_CONFIGS.items():
        token = HARDCODED_TOKENS.get(token_name) or os.getenv(token_name)
        if token:
            bot = create_bot(prefix, f"Bot-{prefix}")
            bots[prefix] = bot
            bot_tokens[prefix] = token
            
            task = asyncio.create_task(bot.start(token))
            bot_tasks.append(task)
            print(f"🚀 Starting {prefix} bot")
        else:
            print(f"⚠️ No token for {prefix} bot")

    if not bot_tasks:
        print("❌ No valid tokens found")
        return

    try:
        await asyncio.gather(*bot_tasks)
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    keep_alive()
    
    print("🤖 Optimized Discord Multi-Bot System")
    print("=" * 50)
    print("New Features:")
    print("• >addbot token prefix - Add bots dynamically")
    print("• >edit prefix \"message\" - Edit spam messages")
    print("• >allow/revoke userid - Manage users")
    print("• Improved auto-restart system")
    print("• Cross-bot command execution")
    print("=" * 50)

    try:
        asyncio.run(run_multiple_bots())
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
    except Exception as e:
        print(f"❌ Startup error: {e}")