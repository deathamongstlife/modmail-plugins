import discord
from discord.ext import commands
import asyncio
import time
from datetime import datetime, timedelta
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

class ModmailInvitePlugin(commands.Cog):
    """
    Modmail plugin for creating configurable Discord invites.
    Provides {invite} and {invite_link} variables for use in responses, snippets, and aliases.
    """
    
    def __init__(self, bot):
        self.bot = bot
        self.db = bot.api.get_plugin_partition(self)
        self.invite_cache = {}  # guild_id -> {url, expires_at}
        self.rate_limits = defaultdict(float)
        
        # Default configuration
        self.defaults = {
            "invite_duration": 86400,  # 1 day in seconds
            "invite_uses": 1,          # Single use
            "auto_create": True,       # Auto-create invites for new threads
            "fallback_channel": None,  # Fallback channel ID if thread channel fails
            "rate_limit_cooldown": 60, # Rate limit cooldown in seconds
            "temporary": False,        # Whether invites grant temporary membership
            "cache_duration": 300      # Cache invites for 5 minutes to reduce API calls
        }
        
        # Start background cleanup task
        self.cleanup_task = asyncio.create_task(self.periodic_cleanup())
        logger.info("ModmailInvitePlugin initialized")
    
    def cog_unload(self):
        """Cleanup when plugin is unloaded"""
        if hasattr(self, 'cleanup_task'):
            self.cleanup_task.cancel()
        logger.info("ModmailInvitePlugin unloaded")
    
    # Configuration Management
    async def get_config(self, guild_id):
        """Retrieve configuration with defaults"""
        config = await self.db.find_one({"_id": f"config_{guild_id}"})
        if not config:
            return self.defaults.copy()
        
        # Merge with defaults for missing keys
        merged = self.defaults.copy()
        merged.update(config.get("settings", {}))
        return merged
    
    async def set_config(self, guild_id, key, value):
        """Update configuration setting with validation"""
        # Validate settings
        if key == "invite_duration":
            if not isinstance(value, int) or value < 60 or value > 604800:  # 1 min to 7 days
                raise ValueError("Duration must be between 60 and 604800 seconds (1 minute to 7 days)")
        elif key == "invite_uses":
            if not isinstance(value, int) or value < 1 or value > 100:
                raise ValueError("Uses must be between 1 and 100")
        elif key == "fallback_channel":
            if value is not None and not isinstance(value, int):
                raise ValueError("Fallback channel must be a channel ID or None")
        elif key == "temporary":
            if not isinstance(value, bool):
                raise ValueError("Temporary must be True or False")
        elif key == "auto_create":
            if not isinstance(value, bool):
                raise ValueError("Auto create must be True or False")
        
        await self.db.update_one(
            {"_id": f"config_{guild_id}"},
            {"$set": {f"settings.{key}": value}},
            upsert=True
        )
        
        # Clear cache for this guild to force refresh
        if guild_id in self.invite_cache:
            del self.invite_cache[guild_id]
    
    # Rate Limiting and Security
    def is_rate_limited(self, guild_id):
        """Check if guild is rate limited"""
        return time.time() < self.rate_limits[guild_id]
    
    def set_rate_limit(self, guild_id, duration):
        """Set rate limit for guild"""
        self.rate_limits[guild_id] = time.time() + duration
    
    async def validate_permissions(self, guild):
        """Validate bot has required permissions"""
        if not guild.me.guild_permissions.create_instant_invite:
            return False, "Bot missing CREATE_INSTANT_INVITE permission"
        return True, None
    
    # Caching System
    def cache_invite(self, guild_id, url, duration):
        """Cache invite with expiration"""
        self.invite_cache[guild_id] = {
            "url": url,
            "expires_at": time.time() + min(duration, 300) - 30  # Cache for max 5 min with 30s buffer
        }
    
    def get_cached_invite(self, guild_id):
        """Retrieve valid cached invite"""
        cached = self.invite_cache.get(guild_id)
        if cached and cached["expires_at"] > time.time():
            return cached["url"]
        
        # Clean expired cache
        if cached:
            del self.invite_cache[guild_id]
        return None
    
    # Invite Creation Logic
    async def create_thread_invite(self, thread, config=None):
        """Create Discord invite with configurable parameters"""
        if not config:
            config = await self.get_config(thread.guild.id)
        
        guild = thread.guild
        
        # Check permissions
        valid, error = await self.validate_permissions(guild)
        if not valid:
            logger.warning(f"Permission error in guild {guild.id}: {error}")
            return None
        
        # Check rate limits
        if self.is_rate_limited(guild.id):
            cached = self.get_cached_invite(guild.id)
            if cached:
                logger.debug(f"Returning cached invite for guild {guild.id}")
                return cached
            logger.warning(f"Rate limited and no cached invite for guild {guild.id}")
            return None
        
        try:
            # Try to create invite on thread channel
            invite = await thread.channel.create_invite(
                max_age=config["invite_duration"],
                max_uses=config["invite_uses"],
                temporary=config.get("temporary", False),
                unique=True,
                reason=f"Modmail invite for thread {thread.id}"
            )
            
            # Cache successful invite
            self.cache_invite(guild.id, invite.url, config["invite_duration"])
            logger.info(f"Created invite for thread {thread.id} in guild {guild.id}")
            return invite.url
            
        except discord.HTTPException as e:
            if e.status == 429:  # Rate limited
                retry_after = getattr(e, 'retry_after', 60)
                self.set_rate_limit(guild.id, retry_after)
                logger.warning(f"Rate limited in guild {guild.id}, retry after {retry_after}s")
                
                # Return cached invite if available
                cached = self.get_cached_invite(guild.id)
                return cached
                
            elif e.status == 403:  # Permission denied
                logger.warning(f"Permission denied for thread channel in guild {guild.id}, trying fallback")
                return await self.try_fallback_channel(guild, config)
            else:
                logger.error(f"HTTP error creating invite in guild {guild.id}: {e}")
                return None
        except Exception as e:
            logger.error(f"Unexpected error creating invite: {e}")
            return None
    
    async def try_fallback_channel(self, guild, config):
        """Attempt invite creation on fallback channels"""
        # Try configured fallback channel first
        fallback_id = config.get("fallback_channel")
        if fallback_id:
            channel = guild.get_channel(fallback_id)
            if channel and channel.permissions_for(guild.me).create_instant_invite:
                try:
                    invite = await channel.create_invite(
                        max_age=config["invite_duration"],
                        max_uses=config["invite_uses"],
                        temporary=config.get("temporary", False),
                        reason="Modmail invite (fallback channel)"
                    )
                    self.cache_invite(guild.id, invite.url, config["invite_duration"])
                    logger.info(f"Created fallback invite in channel {channel.id} for guild {guild.id}")
                    return invite.url
                except discord.HTTPException:
                    logger.warning(f"Failed to create invite in fallback channel {channel.id}")
        
        # Final fallback: first available text channel
        for channel in guild.text_channels:
            if channel.permissions_for(guild.me).create_instant_invite:
                try:
                    invite = await channel.create_invite(
                        max_age=3600,  # 1 hour for emergency fallback
                        max_uses=1,
                        reason="Modmail invite (emergency fallback)"
                    )
                    logger.info(f"Created emergency fallback invite in channel {channel.id} for guild {guild.id}")
                    return invite.url
                except discord.HTTPException:
                    continue
        
        logger.error(f"No channels available for invite creation in guild {guild.id}")
        return None
    
    # Event Handlers
    @commands.Cog.listener()
    async def on_thread_ready(self, thread, creator, category, initial_message):
        """Create invite variable when modmail thread is ready"""
        try:
            config = await self.get_config(thread.guild.id)
            
            if config.get("auto_create", True):
                invite_url = await self.create_thread_invite(thread, config)
                if invite_url:
                    # Register variables for modmail's variable system
                    if not hasattr(thread, '_custom_variables'):
                        thread._custom_variables = {}
                    thread._custom_variables['invite'] = invite_url
                    thread._custom_variables['invite_link'] = invite_url
                    
                    # Store persistently
                    await self.db.update_one(
                        {"_id": f"thread_{thread.id}"},
                        {
                            "$set": {
                                "invite_url": invite_url,
                                "created_at": datetime.utcnow(),
                                "guild_id": thread.guild.id,
                                "thread_id": thread.id
                            }
                        },
                        upsert=True
                    )
                    logger.debug(f"Stored invite variable for thread {thread.id}")
        except Exception as e:
            logger.error(f"Error in on_thread_ready: {e}")
    
    @commands.Cog.listener()
    async def on_thread_close(self, thread, closer, silent, delete_channel):
        """Clean up thread data when thread closes"""
        try:
            await self.db.delete_one({"_id": f"thread_{thread.id}"})
            logger.debug(f"Cleaned up data for closed thread {thread.id}")
        except Exception as e:
            logger.error(f"Error cleaning up thread data: {e}")
    
    # Commands
    @commands.group(name="inviteconfig", invoke_without_command=True)
    @commands.has_permissions(manage_guild=True)
    async def invite_config(self, ctx):
        """Display current invite plugin configuration"""
        config = await self.get_config(ctx.guild.id)
        
        embed = discord.Embed(
            title="🔗 Invite Plugin Configuration",
            color=0x00ff00,
            description="Current settings for Discord invite generation"
        )
        
        # Duration formatting
        duration = config['invite_duration']
        if duration >= 86400:
            duration_str = f"{duration // 86400} day(s)"
        elif duration >= 3600:
            duration_str = f"{duration // 3600} hour(s)"
        else:
            duration_str = f"{duration} second(s)"
        
        embed.add_field(name="Duration", value=duration_str, inline=True)
        embed.add_field(name="Max Uses", value=config['invite_uses'], inline=True)
        embed.add_field(name="Auto Create", value="✅" if config['auto_create'] else "❌", inline=True)
        embed.add_field(name="Temporary", value="✅" if config.get('temporary', False) else "❌", inline=True)
        
        fallback = config.get('fallback_channel')
        if fallback:
            fallback_channel = ctx.guild.get_channel(fallback)
            fallback_str = fallback_channel.mention if fallback_channel else f"Invalid Channel ({fallback})"
        else:
            fallback_str = "None"
        embed.add_field(name="Fallback Channel", value=fallback_str, inline=True)
        
        embed.set_footer(text="Use the subcommands to modify settings")
        await ctx.send(embed=embed)
    
    @invite_config.command(name="duration")
    @commands.has_permissions(manage_guild=True)
    async def set_duration(self, ctx, seconds: int):
        """Set invite duration (60-604800 seconds)"""
        try:
            await self.set_config(ctx.guild.id, "invite_duration", seconds)
            
            # Format duration for display
            if seconds >= 86400:
                duration_str = f"{seconds // 86400} day(s)"
            elif seconds >= 3600:
                duration_str = f"{seconds // 3600} hour(s)"
            else:
                duration_str = f"{seconds} second(s)"
            
            await ctx.send(f"✅ Invite duration set to {duration_str}")
        except ValueError as e:
            await ctx.send(f"❌ {e}")
    
    @invite_config.command(name="uses")
    @commands.has_permissions(manage_guild=True)
    async def set_uses(self, ctx, max_uses: int):
        """Set invite usage limit (1-100)"""
        try:
            await self.set_config(ctx.guild.id, "invite_uses", max_uses)
            await ctx.send(f"✅ Invite usage limit set to {max_uses}")
        except ValueError as e:
            await ctx.send(f"❌ {e}")
    
    @invite_config.command(name="autocreate")
    @commands.has_permissions(manage_guild=True)
    async def set_auto_create(self, ctx, enabled: bool):
        """Enable/disable automatic invite creation for new threads"""
        await self.set_config(ctx.guild.id, "auto_create", enabled)
        status = "enabled" if enabled else "disabled"
        await ctx.send(f"✅ Automatic invite creation {status}")
    
    @invite_config.command(name="temporary")
    @commands.has_permissions(manage_guild=True)
    async def set_temporary(self, ctx, enabled: bool):
        """Set whether invites grant temporary membership"""
        await self.set_config(ctx.guild.id, "temporary", enabled)
        status = "temporary" if enabled else "permanent"
        await ctx.send(f"✅ Invites will now grant {status} membership")
    
    @invite_config.command(name="fallback")
    @commands.has_permissions(manage_guild=True)
    async def set_fallback_channel(self, ctx, channel: discord.TextChannel = None):
        """Set fallback channel for invite creation"""
        channel_id = channel.id if channel else None
        await self.set_config(ctx.guild.id, "fallback_channel", channel_id)
        
        if channel:
            await ctx.send(f"✅ Fallback channel set to {channel.mention}")
        else:
            await ctx.send("✅ Fallback channel cleared")
    
    @commands.command(name="createinvite")
    @commands.has_permissions(manage_guild=True)
    async def create_invite_command(self, ctx, duration: int = None, uses: int = None):
        """Manually create an invite with optional custom settings"""
        config = await self.get_config(ctx.guild.id)
        
        # Override with provided parameters
        if duration is not None:
            if duration < 60 or duration > 604800:
                await ctx.send("❌ Duration must be between 60 and 604800 seconds")
                return
            config["invite_duration"] = duration
        
        if uses is not None:
            if uses < 1 or uses > 100:
                await ctx.send("❌ Uses must be between 1 and 100")
                return
            config["invite_uses"] = uses
        
        # Create a mock thread object for the current channel
        class MockThread:
            def __init__(self, channel, guild):
                self.channel = channel
                self.guild = guild
                self.id = channel.id
        
        mock_thread = MockThread(ctx.channel, ctx.guild)
        invite_url = await self.create_thread_invite(mock_thread, config)
        
        if invite_url:
            embed = discord.Embed(
                title="🔗 Invite Created",
                description=f"[Click here to join]({invite_url})",
                color=0x00ff00
            )
            embed.add_field(name="Duration", value=f"{config['invite_duration']} seconds", inline=True)
            embed.add_field(name="Max Uses", value=config['invite_uses'], inline=True)
            await ctx.send(embed=embed)
        else:
            await ctx.send("❌ Failed to create invite. Check bot permissions.")
    
    @commands.command(name="invitestats")
    @commands.has_permissions(manage_guild=True)
    async def invite_stats(self, ctx):
        """Display invite plugin statistics"""
        try:
            # Count active threads with invites
            active_threads = await self.db.count_documents({
                "_id": {"$regex": "^thread_"},
                "guild_id": ctx.guild.id
            })
            
            # Count cached invites
            cached_count = len([k for k in self.invite_cache.keys() if k == ctx.guild.id])
            
            embed = discord.Embed(
                title="📊 Invite Plugin Statistics",
                color=0x0099ff
            )
            embed.add_field(name="Active Thread Invites", value=active_threads, inline=True)
            embed.add_field(name="Cached Invites", value=cached_count, inline=True)
            
            # Rate limit status
            if self.is_rate_limited(ctx.guild.id):
                remaining = max(0, int(self.rate_limits[ctx.guild.id] - time.time()))
                embed.add_field(name="Rate Limit", value=f"{remaining}s remaining", inline=True)
            else:
                embed.add_field(name="Rate Limit", value="✅ Clear", inline=True)
            
            await ctx.send(embed=embed)
        except Exception as e:
            await ctx.send(f"❌ Error retrieving statistics: {e}")
    
    # Maintenance Tasks
    async def periodic_cleanup(self):
        """Background task for cleaning up expired data"""
        while not self.bot.is_closed():
            try:
                await self.cleanup_expired_data()
                await asyncio.sleep(3600)  # Run every hour
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic cleanup: {e}")
                await asyncio.sleep(3600)
    
    async def cleanup_expired_data(self):
        """Remove expired thread data and clean cache"""
        try:
            cutoff = datetime.utcnow() - timedelta(days=7)
            
            # Remove old thread data
            result = await self.db.delete_many({
                "_id": {"$regex": "^thread_"},
                "created_at": {"$lt": cutoff}
            })
            
            if result.deleted_count > 0:
                logger.info(f"Cleaned up {result.deleted_count} expired thread records")
            
            # Clean expired cache entries
            current_time = time.time()
            expired_keys = [
                k for k, v in self.invite_cache.items() 
                if v["expires_at"] <= current_time
            ]
            
            for key in expired_keys:
                del self.invite_cache[key]
            
            if expired_keys:
                logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")
                
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

# Plugin setup function - required by modmail
async def setup(bot):
    """Setup function called by modmail when loading the plugin"""
    await bot.add_cog(ModmailInvitePlugin(bot))
    logger.info("ModmailInvitePlugin loaded successfully")
