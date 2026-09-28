/**
 * ALL-IN-ONE DISCORD BOT — single file (index.js)
 * Discord.js v14
 *
 * Persistence: flat JSON file (./data/db.json) with atomic crash-safe saves,
 * plus a SQLite database (./data/tickets.db) for the ticket system.
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const https = require('https');
const express = require('express');
const Database = require('better-sqlite3');

const {
  Client, GatewayIntentBits, Partials, Collection, REST, Routes,
  SlashCommandBuilder, PermissionFlagsBits, EmbedBuilder, ActionRowBuilder,
  ButtonBuilder, ButtonStyle, StringSelectMenuBuilder, ModalBuilder,
  TextInputBuilder, TextInputStyle, ChannelType, AttachmentBuilder,
  PermissionsBitField, MessageFlags,
} = require('discord.js');

// ---------------------------------------------------------------------------
// DATABASE (JSON file, in-memory cache + atomic crash-safe save)
// ---------------------------------------------------------------------------
const DATA_DIR = path.join(__dirname, 'data');
const DB_FILE = path.join(DATA_DIR, 'db.json');
const DB_BAK_FILE = path.join(DATA_DIR, 'db.backup.json');
const DB_TMP_FILE = path.join(DATA_DIR, 'db.tmp.json');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const DEFAULT_DB = () => ({
  guilds: {},
});

function loadDB() {
  if (!fs.existsSync(DB_FILE)) {
    fs.writeFileSync(DB_FILE, JSON.stringify(DEFAULT_DB(), null, 2));
  }
  try {
    return JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
  } catch (e) {
    console.error('Failed to parse db.json, attempting backup recovery.', e);
    if (fs.existsSync(DB_BAK_FILE)) {
      try {
        const recovered = JSON.parse(fs.readFileSync(DB_BAK_FILE, 'utf8'));
        console.log('Recovered database from backup file.');
        fs.writeFileSync(DB_FILE, JSON.stringify(recovered, null, 2));
        return recovered;
      } catch (e2) {
        console.error('Backup file also corrupted, reinitializing.', e2);
      }
    }
    const fresh = DEFAULT_DB();
    fs.writeFileSync(DB_FILE, JSON.stringify(fresh, null, 2));
    return fresh;
  }
}

const db = loadDB();
let saveTimer = null;
let pendingSave = false;

function flushDBSync() {
  try {
    const json = JSON.stringify(db, null, 2);
    if (fs.existsSync(DB_FILE)) {
      fs.copyFileSync(DB_FILE, DB_BAK_FILE);
    }
    fs.writeFileSync(DB_TMP_FILE, json);
    fs.renameSync(DB_TMP_FILE, DB_FILE);
    pendingSave = false;
  } catch (err) {
    console.error('DB save error:', err);
  }
}

function saveDB() {
  pendingSave = true;
  clearTimeout(saveTimer);
  saveTimer = setTimeout(flushDBSync, 250);
}

setInterval(() => { if (pendingSave) flushDBSync(); }, 30 * 1000);

function gracefulExit(signal) {
  console.log(`Received ${signal}, flushing database before exit...`);
  flushDBSync();
  process.exit(0);
}
process.on('SIGINT', () => gracefulExit('SIGINT'));
process.on('SIGTERM', () => gracefulExit('SIGTERM'));

function defaultGuildConfig() {
  return {
    superAdmins: [],
    extraOwners: [],
    protectedExtra: [],
    antinuke: {
      enabled: false,
      whitelist: [],
      thresholds: { channelDelete: 2, channelCreate: 3, roleDelete: 2, roleCreate: 3, ban: 2, kick: 3, webhook: 2 },
      windowMs: 8000,
      logChannel: null,
      logs: [],
    },
    antispam: {
      enabled: false,
      logChannel: null,
      msgLimit: 5,
      msgWindowMs: 5000,
      duplicateWindowMs: 15000,
      mentionLimit: 4,
      capsPercent: 65,
      linkLimit: 2,
      blockInvites: true,
      repeatedCharLimit: 8,
    },
    badwords: [],
    warnings: {},
    tickets: {
      categoryId: null,
      supportRoles: [],
      panels: {},
      types: {},
      openTickets: {},
      userTicketCount: {},
      logChannel: null,
      nextPanelId: 1,
      closedCount: 0,
    },
    welcome: { enabled: false, channelId: null, message: 'Welcome {user} to {server}! You are member #{membercount}.', embed: true, banner: null },
    goodbye: { enabled: false, channelId: null, message: '{username} has left {server}. We now have {membercount} members.', embed: true, banner: null },
    autorole: { roleId: null },
    stickyroles: { enabled: false, store: {} },
    verification: { enabled: false, roleId: null, channelId: null, messageId: null },
    serverstats: { channels: {} },
    starboard: { enabled: false, channelId: null, threshold: 3, messages: {} },
    reactionroles: [],
    autopublish: { channels: [] },
    logging: { channelId: null },
    dmlogs: [],
    invites: { cache: {}, joins: {}, leaders: {} },
    customcommands: {},
    giveaways: {},
    statusmonitors: [],
    reminders: [],
    afk: {},
    ai: {
      customPrompt: '',
      memory: {},
    },
    applications: {
      reviewChannelId: null,
      resultDMs: true,
      questions: ['Why do you want to join the team?', 'Relevant experience?', 'How old are you?', 'Timezone?'],
      panels: {},
      nextPanelId: 1,
      pendingByUser: {},
      submissions: [],
    },
  };
}

function getGuild(guildId) {
  if (!db.guilds[guildId]) {
    db.guilds[guildId] = defaultGuildConfig();
    saveDB();
  } else {
    const def = defaultGuildConfig();
    for (const k of Object.keys(def)) {
      if (db.guilds[guildId][k] === undefined) db.guilds[guildId][k] = def[k];
    }
  }
  return db.guilds[guildId];
}

// ---------------------------------------------------------------------------
// SQLITE DATABASE — TICKET SYSTEM
// ---------------------------------------------------------------------------
const SQLITE_FILE = path.join(DATA_DIR, 'tickets.db');
const sqlite = new Database(SQLITE_FILE);
sqlite.pragma('journal_mode = WAL');

sqlite.exec(`
  CREATE TABLE IF NOT EXISTS tickets (
    channel_id   TEXT PRIMARY KEY,
    guild_id     TEXT NOT NULL,
    user_id      TEXT NOT NULL,
    type         TEXT DEFAULT 'general',
    claimed_by   TEXT,
    status       TEXT DEFAULT 'open',
    created_at   INTEGER NOT NULL,
    closed_at    INTEGER,
    closed_by    TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_tickets_guild ON tickets(guild_id);
  CREATE INDEX IF NOT EXISTS idx_tickets_user ON tickets(guild_id, user_id);
  CREATE INDEX IF NOT EXISTS idx_tickets_status ON tickets(guild_id, status);

  CREATE TABLE IF NOT EXISTS ticket_transcripts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id   TEXT NOT NULL,
    guild_id     TEXT NOT NULL,
    author_tag   TEXT,
    content      TEXT,
    attachments  TEXT,
    created_at   INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_transcripts_channel ON ticket_transcripts(channel_id);
`);

const ticketDB = {
  create: sqlite.prepare(`INSERT INTO tickets (channel_id, guild_id, user_id, type, created_at, status) VALUES (?, ?, ?, ?, ?, 'open')`),
  getOpenByChannel: sqlite.prepare(`SELECT * FROM tickets WHERE channel_id = ? AND status = 'open'`),
  getByChannel: sqlite.prepare(`SELECT * FROM tickets WHERE channel_id = ?`),
  countOpenByUser: sqlite.prepare(`SELECT COUNT(*) AS c FROM tickets WHERE guild_id = ? AND user_id = ? AND status = 'open'`),
  claim: sqlite.prepare(`UPDATE tickets SET claimed_by = ? WHERE channel_id = ?`),
  close: sqlite.prepare(`UPDATE tickets SET status = 'closed', closed_at = ?, closed_by = ? WHERE channel_id = ?`),
  allOpenByGuild: sqlite.prepare(`SELECT * FROM tickets WHERE guild_id = ? AND status = 'open'`),
  countClosedByGuild: sqlite.prepare(`SELECT COUNT(*) AS c FROM tickets WHERE guild_id = ? AND status = 'closed'`),
  countOpenByGuild: sqlite.prepare(`SELECT COUNT(*) AS c FROM tickets WHERE guild_id = ? AND status = 'open'`),
  addTranscriptLine: sqlite.prepare(`INSERT INTO ticket_transcripts (channel_id, guild_id, author_tag, content, attachments, created_at) VALUES (?, ?, ?, ?, ?, ?)`),
  getTranscript: sqlite.prepare(`SELECT * FROM ticket_transcripts WHERE channel_id = ? ORDER BY created_at ASC`),
};

// ---------------------------------------------------------------------------
// CLIENT
// ---------------------------------------------------------------------------
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildModeration,
    GatewayIntentBits.GuildInvites,
    GatewayIntentBits.GuildMessageReactions,
    GatewayIntentBits.GuildPresences,
    GatewayIntentBits.GuildWebhooks,
  ],
  partials: [Partials.Message, Partials.Channel, Partials.Reaction, Partials.GuildMember, Partials.User],
});

client.cooldowns = new Collection();
const startTime = Date.now();

// ---------------------------------------------------------------------------
// PERMISSION HELPERS
// ---------------------------------------------------------------------------
const LEVEL = { USER: 1, MOD: 2, ADMIN: 3, EXTRA_OWNER: 4, SUPER_ADMIN: 5, OWNER: 6 };

const BOT_OWNER_IDS = (process.env.BOT_OWNER_IDS || '').split(',').map(s => s.trim()).filter(Boolean);
function isBotOwner(userId) {
  return BOT_OWNER_IDS.includes(userId);
}

function isProtected(guild, gconf, userId) {
  if (isBotOwner(userId)) return true;
  if (userId === guild.ownerId) return true;
  if (gconf.extraOwners.includes(userId)) return true;
  if (gconf.superAdmins.includes(userId)) return true;
  if (gconf.protectedExtra.includes(userId)) return true;
  return false;
}

function getLevel(guild, gconf, member) {
  if (!member) return LEVEL.USER;
  if (isBotOwner(member.id)) return LEVEL.OWNER;
  if (member.id === guild.ownerId) return LEVEL.OWNER;
  if (gconf.superAdmins.includes(member.id)) return LEVEL.SUPER_ADMIN;
  if (gconf.extraOwners.includes(member.id)) return LEVEL.EXTRA_OWNER;
  if (member.permissions?.has(PermissionFlagsBits.Administrator)) return LEVEL.SUPER_ADMIN;
  if (member.permissions?.has(PermissionFlagsBits.ModerateMembers) || member.permissions?.has(PermissionFlagsBits.KickMembers)) return LEVEL.MOD;
  return LEVEL.USER;
}

function requireLevel(interaction, gconf, min) {
  const lvl = getLevel(interaction.guild, gconf, interaction.member);
  return lvl >= min;
}

function isBotSuperAdmin(interaction, gconf) {
  return getLevel(interaction.guild, gconf, interaction.member) >= LEVEL.SUPER_ADMIN;
}

function botCanActOn(guild, targetMember) {
  const me = guild.members.me;
  if (!me) return false;
  if (targetMember.id === guild.ownerId) return false;
  return me.roles.highest.comparePositionTo(targetMember.roles.highest) > 0;
}

function actorOutranks(actorMember, targetMember, guild, gconf) {
  if (actorMember.id === guild.ownerId) return true;
  if (gconf && (gconf.superAdmins.includes(actorMember.id) || gconf.extraOwners.includes(actorMember.id))) return true;
  return actorMember.roles.highest.comparePositionTo(targetMember.roles.highest) > 0;
}

// ---------------------------------------------------------------------------
// EMBED HELPERS
// ---------------------------------------------------------------------------
const COLORS = { success: 0x57F287, error: 0xED4245, warning: 0xFEE75C, info: 0x5865F2, neutral: 0x2B2D31 };

function successEmbed(desc, title = 'Success') {
  return new EmbedBuilder().setColor(COLORS.success).setTitle(`✅ ${title}`).setDescription(desc).setTimestamp();
}
function errorEmbed(desc, title = 'Error') {
  return new EmbedBuilder().setColor(COLORS.error).setTitle(`❌ ${title}`).setDescription(desc).setTimestamp();
}
function warnEmbed(desc, title = 'Warning') {
  return new EmbedBuilder().setColor(COLORS.warning).setTitle(`⚠️ ${title}`).setDescription(desc).setTimestamp();
}
function infoEmbed(desc, title = 'Info') {
  return new EmbedBuilder().setColor(COLORS.info).setTitle(title).setDescription(desc).setTimestamp();
}

async function safeReply(interaction, payload) {
  try {
    if (interaction.deferred || interaction.replied) {
      return await interaction.editReply(payload);
    }
    return await interaction.reply(payload);
  } catch (e) {
    console.error('safeReply error:', e?.message);
  }
}

async function logEvent(guild, gconf, embed) {
  if (!gconf.logging.channelId) return;
  try {
    const ch = await guild.channels.fetch(gconf.logging.channelId).catch(() => null);
    if (ch) await ch.send({ embeds: [embed] });
  } catch (e) { /* ignore */ }
}

function replaceVars(str, { user, guild, memberCount }) {
  return str
    .replaceAll('{user}', user ? `<@${user.id}>` : '')
    .replaceAll('{username}', user ? user.username : '')
    .replaceAll('{server}', guild ? guild.name : '')
    .replaceAll('{membercount}', String(memberCount ?? guild?.memberCount ?? ''));
}

// ---------------------------------------------------------------------------
// SLASH COMMAND DEFINITIONS
// ---------------------------------------------------------------------------
const commands = [];
function cmd(builder) { commands.push(builder); return builder; }

cmd(new SlashCommandBuilder().setName('superadmin').setDescription('Manage bot super admins')
  .addSubcommand(s => s.setName('add').setDescription('Add a super admin').addUserOption(o => o.setName('user').setDescription('User').setRequired(true)))
  .addSubcommand(s => s.setName('remove').setDescription('Remove a super admin').addUserOption(o => o.setName('user').setDescription('User').setRequired(true)))
  .addSubcommand(s => s.setName('list').setDescription('List super admins')));

cmd(new SlashCommandBuilder().setName('extraowner').setDescription('Manage bot extra owners')
  .addSubcommand(s => s.setName('add').setDescription('Add an extra owner').addUserOption(o => o.setName('user').setDescription('User').setRequired(true)))
  .addSubcommand(s => s.setName('remove').setDescription('Remove an extra owner').addUserOption(o => o.setName('user').setDescription('User').setRequired(true)))
  .addSubcommand(s => s.setName('list').setDescription('List extra owners')));

cmd(new SlashCommandBuilder().setName('botconfig').setDescription('Open the interactive bot configuration panel'));

cmd(new SlashCommandBuilder().setName('antinuke').setDescription('Anti-nuke protection system')
  .addSubcommand(s => s.setName('enable').setDescription('Enable anti-nuke'))
  .addSubcommand(s => s.setName('disable').setDescription('Disable anti-nuke'))
  .addSubcommand(s => s.setName('config').setDescription('View/edit anti-nuke thresholds')
    .addStringOption(o => o.setName('setting').setDescription('Threshold to change').addChoices(
      { name: 'channelDelete', value: 'channelDelete' }, { name: 'channelCreate', value: 'channelCreate' },
      { name: 'roleDelete', value: 'roleDelete' }, { name: 'roleCreate', value: 'roleCreate' },
      { name: 'ban', value: 'ban' }, { name: 'kick', value: 'kick' }, { name: 'webhook', value: 'webhook' },
      { name: 'windowMs', value: 'windowMs' }, { name: 'logChannel', value: 'logChannel' },
    ))
    .addStringOption(o => o.setName('value').setDescription('New value (number, or #channel mention for logChannel)')))
  .addSubcommandGroup(g => g.setName('whitelist').setDescription('Manage anti-nuke whitelist')
    .addSubcommand(s => s.setName('add').setDescription('Whitelist a user').addUserOption(o => o.setName('user').setDescription('User').setRequired(true)))
    .addSubcommand(s => s.setName('remove').setDescription('Remove from whitelist').addUserOption(o => o.setName('user').setDescription('User').setRequired(true)))
    .addSubcommand(s => s.setName('list').setDescription('List whitelist')))
  .addSubcommand(s => s.setName('logs').setDescription('Show recent anti-nuke events')));

cmd(new SlashCommandBuilder().setName('antispam').setDescription('Anti-spam / automod system')
  .addSubcommand(s => s.setName('enable').setDescription('Enable anti-spam'))
  .addSubcommand(s => s.setName('disable').setDescription('Disable anti-spam'))
  .addSubcommand(s => s.setName('config').setDescription('Edit anti-spam thresholds')
    .addStringOption(o => o.setName('setting').setDescription('Setting to change').addChoices(
      { name: 'msgLimit', value: 'msgLimit' }, { name: 'msgWindowMs', value: 'msgWindowMs' },
      { name: 'mentionLimit', value: 'mentionLimit' }, { name: 'capsPercent', value: 'capsPercent' },
      { name: 'linkLimit', value: 'linkLimit' }, { name: 'blockInvites', value: 'blockInvites' },
      { name: 'repeatedCharLimit', value: 'repeatedCharLimit' }, { name: 'logChannel', value: 'logChannel' },
    ).setRequired(true))
    .addStringOption(o => o.setName('value').setDescription('New value').setRequired(true))));

cmd(new SlashCommandBuilder().setName('badwords').setDescription('Manage the bad word filter')
  .addSubcommand(s => s.setName('add').setDescription('Add a word').addStringOption(o => o.setName('word').setDescription('Word').setRequired(true)))
  .addSubcommand(s => s.setName('remove').setDescription('Remove a word').addStringOption(o => o.setName('word').setDescription('Word').setRequired(true)))
  .addSubcommand(s => s.setName('list').setDescription('List filtered words (DM only)')));

cmd(new SlashCommandBuilder().setName('ban').setDescription('Ban a member')
  .addUserOption(o => o.setName('user').setDescription('User to ban').setRequired(true))
  .addStringOption(o => o.setName('reason').setDescription('Reason')));
cmd(new SlashCommandBuilder().setName('kick').setDescription('Kick a member')
  .addUserOption(o => o.setName('user').setDescription('User to kick').setRequired(true))
  .addStringOption(o => o.setName('reason').setDescription('Reason')));
cmd(new SlashCommandBuilder().setName('timeout').setDescription('Timeout a member')
  .addUserOption(o => o.setName('user').setDescription('User').setRequired(true))
  .addStringOption(o => o.setName('duration').setDescription('e.g. 10m, 1h, 1d').setRequired(true))
  .addStringOption(o => o.setName('reason').setDescription('Reason')));
cmd(new SlashCommandBuilder().setName('warn').setDescription('Warn a member')
  .addUserOption(o => o.setName('user').setDescription('User').setRequired(true))
  .addStringOption(o => o.setName('reason').setDescription('Reason').setRequired(true)));
cmd(new SlashCommandBuilder().setName('warnings').setDescription('View warnings for a member')
  .addUserOption(o => o.setName('user').setDescription('User').setRequired(true)));
cmd(new SlashCommandBuilder().setName('clearwarns').setDescription('Clear warnings for a member')
  .addUserOption(o => o.setName('user').setDescription('User').setRequired(true)));
cmd(new SlashCommandBuilder().setName('purge').setDescription('Bulk delete messages')
  .addIntegerOption(o => o.setName('amount').setDescription('1-100').setRequired(true).setMinValue(1).setMaxValue(100)));
cmd(new SlashCommandBuilder().setName('lock').setDescription('Lock the current channel'));
cmd(new SlashCommandBuilder().setName('unlock').setDescription('Unlock the current channel'));
cmd(new SlashCommandBuilder().setName('slowmode').setDescription('Set slowmode for this channel')
  .addIntegerOption(o => o.setName('seconds').setDescription('0-21600').setRequired(true).setMinValue(0).setMaxValue(21600)));

cmd(new SlashCommandBuilder().setName('ticket').setDescription('Ticket system')
  .addSubcommand(s => s.setName('setup').setDescription('Configure ticket category & support role')
    .addChannelOption(o => o.setName('category').setDescription('Category for tickets').addChannelTypes(ChannelType.GuildCategory).setRequired(true))
    .addRoleOption(o => o.setName('supportrole').setDescription('Support role').setRequired(true))
    .addChannelOption(o => o.setName('logchannel').setDescription('Ticket log channel')))
  .addSubcommand(s => s.setName('panel').setDescription('Post a ticket creation panel')
    .addStringOption(o => o.setName('title').setDescription('Panel title').setRequired(true))
    .addStringOption(o => o.setName('description').setDescription('Panel description').setRequired(true))
    .addStringOption(o => o.setName('banner').setDescription('Banner image URL')))
  .addSubcommand(s => s.setName('panels').setDescription('List ticket panels'))
  .addSubcommand(s => s.setName('editpanel').setDescription('Edit a panel')
    .addIntegerOption(o => o.setName('id').setDescription('Panel ID').setRequired(true))
    .addStringOption(o => o.setName('title').setDescription('New title'))
    .addStringOption(o => o.setName('description').setDescription('New description'))
    .addStringOption(o => o.setName('banner').setDescription('New banner image URL')))
  .addSubcommand(s => s.setName('deletepanel').setDescription('Delete a panel')
    .addIntegerOption(o => o.setName('id').setDescription('Panel ID').setRequired(true)))
  .addSubcommand(s => s.setName('closeall').setDescription('Close all open tickets'))
  .addSubcommand(s => s.setName('add').setDescription('Add a user to this ticket').addUserOption(o => o.setName('user').setDescription('User').setRequired(true)))
  .addSubcommand(s => s.setName('remove').setDescription('Remove a user from this ticket').addUserOption(o => o.setName('user').setDescription('User').setRequired(true)))
  .addSubcommand(s => s.setName('close').setDescription('Close this ticket'))
  .addSubcommand(s => s.setName('claim').setDescription('Claim this ticket'))
  .addSubcommand(s => s.setName('transcript').setDescription('Generate a transcript for this ticket'))
  .addSubcommand(s => s.setName('stats').setDescription('Show ticket statistics'))
  .addSubcommand(s => s.setName('addtype').setDescription('Add a ticket type')
    .addStringOption(o => o.setName('name').setDescription('Type name').setRequired(true))
    .addStringOption(o => o.setName('emoji').setDescription('Emoji')))
  .addSubcommand(s => s.setName('listtypes').setDescription('List ticket types'))
  .addSubcommand(s => s.setName('edittype').setDescription('Edit a ticket type')
    .addStringOption(o => o.setName('name').setDescription('Type name').setRequired(true))
    .addStringOption(o => o.setName('emoji').setDescription('New emoji').setRequired(true)))
  .addSubcommand(s => s.setName('deletetype').setDescription('Delete a ticket type')
    .addStringOption(o => o.setName('name').setDescription('Type name').setRequired(true)))
  .addSubcommand(s => s.setName('config').setDescription('Show ticket configuration')));

cmd(new SlashCommandBuilder().setName('welcome').setDescription('Welcome message system')
  .addSubcommand(s => s.setName('setup').setDescription('Configure welcome messages')
    .addChannelOption(o => o.setName('channel').setDescription('Channel').setRequired(true))
    .addStringOption(o => o.setName('message').setDescription('Message ({user},{username},{server},{membercount})'))
    .addStringOption(o => o.setName('banner').setDescription('Banner image URL')))
  .addSubcommand(s => s.setName('test').setDescription('Send a test welcome message'))
  .addSubcommand(s => s.setName('disable').setDescription('Disable welcome messages')));

cmd(new SlashCommandBuilder().setName('goodbye').setDescription('Goodbye message system')
  .addSubcommand(s => s.setName('setup').setDescription('Configure goodbye messages')
    .addChannelOption(o => o.setName('channel').setDescription('Channel').setRequired(true))
    .addStringOption(o => o.setName('message').setDescription('Message ({user},{username},{server},{membercount})'))
    .addStringOption(o => o.setName('banner').setDescription('Banner image URL')))
  .addSubcommand(s => s.setName('test').setDescription('Send a test goodbye message'))
  .addSubcommand(s => s.setName('disable').setDescription('Disable goodbye messages')));

cmd(new SlashCommandBuilder().setName('dm').setDescription('DM system')
  .addSubcommand(s => s.setName('user').setDescription('DM a single user')
    .addUserOption(o => o.setName('user').setDescription('User').setRequired(true))
    .addStringOption(o => o.setName('message').setDescription('Message').setRequired(true)))
  .addSubcommand(s => s.setName('role').setDescription('DM all members with a role')
    .addRoleOption(o => o.setName('role').setDescription('Role').setRequired(true))
    .addStringOption(o => o.setName('message').setDescription('Message').setRequired(true)))
  .addSubcommand(s => s.setName('everyone').setDescription('DM all server members (rate-limited, slow)')
    .addStringOption(o => o.setName('message').setDescription('Message').setRequired(true))));
cmd(new SlashCommandBuilder().setName('dmlogs').setDescription('Show recent DM activity'));

cmd(new SlashCommandBuilder().setName('invites').setDescription("Show your (or another user's) invite stats")
  .addUserOption(o => o.setName('user').setDescription('User')));
cmd(new SlashCommandBuilder().setName('inviteleaderboard').setDescription('Show invite leaderboard'));
cmd(new SlashCommandBuilder().setName('resetinvites').setDescription('Reset all invite statistics'));

cmd(new SlashCommandBuilder().setName('customcommand').setDescription('Manage custom commands')
  .addSubcommand(s => s.setName('add').setDescription('Add a custom command')
    .addStringOption(o => o.setName('name').setDescription('Trigger name').setRequired(true))
    .addStringOption(o => o.setName('response').setDescription('Response text').setRequired(true)))
  .addSubcommand(s => s.setName('remove').setDescription('Remove a custom command').addStringOption(o => o.setName('name').setDescription('Trigger name').setRequired(true)))
  .addSubcommand(s => s.setName('list').setDescription('List custom commands')));

cmd(new SlashCommandBuilder().setName('giveaway').setDescription('Giveaway system')
  .addSubcommand(s => s.setName('start').setDescription('Start a giveaway')
    .addStringOption(o => o.setName('prize').setDescription('Prize').setRequired(true))
    .addStringOption(o => o.setName('duration').setDescription('e.g. 10m, 1h, 1d').setRequired(true))
    .addIntegerOption(o => o.setName('winners').setDescription('Number of winners').setRequired(true).setMinValue(1)))
  .addSubcommand(s => s.setName('end').setDescription('End a giveaway early').addStringOption(o => o.setName('messageid').setDescription('Giveaway message ID').setRequired(true)))
  .addSubcommand(s => s.setName('reroll').setDescription('Reroll a giveaway winner').addStringOption(o => o.setName('messageid').setDescription('Giveaway message ID').setRequired(true))));

cmd(new SlashCommandBuilder().setName('statusmonitor').setDescription('Website status monitor')
  .addSubcommand(s => s.setName('add').setDescription('Add a URL to monitor')
    .addStringOption(o => o.setName('url').setDescription('URL (https://...)').setRequired(true))
    .addChannelOption(o => o.setName('channel').setDescription('Channel for status updates').setRequired(true)))
  .addSubcommand(s => s.setName('remove').setDescription('Remove a monitored URL').addStringOption(o => o.setName('url').setDescription('URL').setRequired(true)))
  .addSubcommand(s => s.setName('list').setDescription('List monitored URLs')));

cmd(new SlashCommandBuilder().setName('weather').setDescription('Get weather for a location').addStringOption(o => o.setName('location').setDescription('City name').setRequired(true)));
cmd(new SlashCommandBuilder().setName('qrcode').setDescription('Generate a QR code').addStringOption(o => o.setName('text').setDescription('Text or URL to encode').setRequired(true)));
cmd(new SlashCommandBuilder().setName('remindme').setDescription('Set a reminder')
  .addStringOption(o => o.setName('when').setDescription('e.g. 10m, 1h, 2d').setRequired(true))
  .addStringOption(o => o.setName('text').setDescription('What to remind you about').setRequired(true)));
cmd(new SlashCommandBuilder().setName('poll').setDescription('Create a poll')
  .addStringOption(o => o.setName('question').setDescription('Poll question').setRequired(true))
  .addStringOption(o => o.setName('options').setDescription('Comma-separated options (max 5)')));
cmd(new SlashCommandBuilder().setName('afk').setDescription('Set your AFK status').addStringOption(o => o.setName('reason').setDescription('Reason')));

cmd(new SlashCommandBuilder().setName('serverinfo').setDescription('Show server information'));
cmd(new SlashCommandBuilder().setName('userinfo').setDescription('Show user information').addUserOption(o => o.setName('user').setDescription('User')));
cmd(new SlashCommandBuilder().setName('roleinfo').setDescription('Show role information').addRoleOption(o => o.setName('role').setDescription('Role').setRequired(true)));
cmd(new SlashCommandBuilder().setName('avatar').setDescription("Show a user's avatar").addUserOption(o => o.setName('user').setDescription('User')));
cmd(new SlashCommandBuilder().setName('banner').setDescription("Show a user's banner").addUserOption(o => o.setName('user').setDescription('User')));
cmd(new SlashCommandBuilder().setName('membercount').setDescription('Show member count statistics'));
cmd(new SlashCommandBuilder().setName('ping').setDescription('Show bot latency'));
cmd(new SlashCommandBuilder().setName('stats').setDescription('Show bot statistics'));
cmd(new SlashCommandBuilder().setName('help').setDescription('Show the help menu'));

cmd(new SlashCommandBuilder().setName('autorole').setDescription('Automatic role on join')
  .addSubcommand(s => s.setName('set').setDescription('Set the autorole').addRoleOption(o => o.setName('role').setDescription('Role').setRequired(true)))
  .addSubcommand(s => s.setName('remove').setDescription('Remove the autorole')));

cmd(new SlashCommandBuilder().setName('stickyroles').setDescription('Sticky roles system')
  .addSubcommand(s => s.setName('enable').setDescription('Enable sticky roles'))
  .addSubcommand(s => s.setName('disable').setDescription('Disable sticky roles')));

cmd(new SlashCommandBuilder().setName('addrole').setDescription('Add a role to a member')
  .addUserOption(o => o.setName('user').setDescription('User').setRequired(true))
  .addRoleOption(o => o.setName('role').setDescription('Role').setRequired(true)));
cmd(new SlashCommandBuilder().setName('removerole').setDescription('Remove a role from a member')
  .addUserOption(o => o.setName('user').setDescription('User').setRequired(true))
  .addRoleOption(o => o.setName('role').setDescription('Role').setRequired(true)));

cmd(new SlashCommandBuilder().setName('verifyconfig').setDescription('Configure the verification system')
  .addRoleOption(o => o.setName('role').setDescription('Role to grant on verify').setRequired(true))
  .addChannelOption(o => o.setName('channel').setDescription('Verification channel').setRequired(true)));
cmd(new SlashCommandBuilder().setName('verify').setDescription('Post the verification button in the configured channel'));

cmd(new SlashCommandBuilder().setName('serverstats').setDescription('Live statistic voice channels')
  .addSubcommand(s => s.setName('setup').setDescription('Create statistic channels'))
  .addSubcommand(s => s.setName('remove').setDescription('Remove statistic channels')));

cmd(new SlashCommandBuilder().setName('starboard').setDescription('Starboard system')
  .addSubcommand(s => s.setName('setup').setDescription('Configure the starboard')
    .addChannelOption(o => o.setName('channel').setDescription('Starboard channel').setRequired(true))
    .addIntegerOption(o => o.setName('threshold').setDescription('Star threshold').setRequired(true).setMinValue(1)))
  .addSubcommand(s => s.setName('remove').setDescription('Disable the starboard')));

cmd(new SlashCommandBuilder().setName('reactionrole').setDescription('Reaction role system')
  .addSubcommand(s => s.setName('add').setDescription('Add a reaction role')
    .addStringOption(o => o.setName('messageid').setDescription('Message ID').setRequired(true))
    .addStringOption(o => o.setName('emoji').setDescription('Emoji').setRequired(true))
    .addRoleOption(o => o.setName('role').setDescription('Role').setRequired(true)))
  .addSubcommand(s => s.setName('remove').setDescription('Remove a reaction role')
    .addStringOption(o => o.setName('messageid').setDescription('Message ID').setRequired(true))
    .addStringOption(o => o.setName('emoji').setDescription('Emoji').setRequired(true)))
  .addSubcommand(s => s.setName('list').setDescription('List reaction roles')));

cmd(new SlashCommandBuilder().setName('pingstatus').setDescription('Start a live ping status message that updates every minute in this channel'));

cmd(new SlashCommandBuilder().setName('autopublish').setDescription('Auto-publish announcement channels')
  .addSubcommand(s => s.setName('setup').setDescription('Add an announcement channel to auto-publish').addChannelOption(o => o.setName('channel').setDescription('Announcement channel').setRequired(true)))
  .addSubcommand(s => s.setName('remove').setDescription('Remove an auto-publish channel').addChannelOption(o => o.setName('channel').setDescription('Channel').setRequired(true))));

cmd(new SlashCommandBuilder().setName('application').setDescription('Application system (staff/member applications)')
  .addSubcommand(s => s.setName('setup').setDescription('Configure where applications are reviewed')
    .addChannelOption(o => o.setName('reviewchannel').setDescription('Channel where submitted applications are posted').setRequired(true)))
  .addSubcommand(s => s.setName('panel').setDescription('Post an application panel with an Apply button')
    .addStringOption(o => o.setName('title').setDescription('Panel title').setRequired(true))
    .addStringOption(o => o.setName('description').setDescription('Panel description').setRequired(true))
    .addStringOption(o => o.setName('banner').setDescription('Banner image URL')))
  .addSubcommand(s => s.setName('addquestion').setDescription('Add a question (max 5 total, modal limit)').addStringOption(o => o.setName('question').setDescription('Question text').setRequired(true)))
  .addSubcommand(s => s.setName('removequestion').setDescription('Remove a question by number').addIntegerOption(o => o.setName('number').setDescription('Question number (from /application listquestions)').setRequired(true)))
  .addSubcommand(s => s.setName('listquestions').setDescription('List configured application questions'))
  .addSubcommand(s => s.setName('list').setDescription('List recent applications').addStringOption(o => o.setName('status').setDescription('Filter by status').addChoices({ name: 'pending', value: 'pending' }, { name: 'accepted', value: 'accepted' }, { name: 'denied', value: 'denied' }))));

cmd(new SlashCommandBuilder().setName('aiconfig').setDescription('Configure the AI (Jarvis) system — Super Admin only')
  .addSubcommand(s => s.setName('setprompt').setDescription('Set a custom addition to the AI system prompt').addStringOption(o => o.setName('prompt').setDescription('Custom instructions for the AI').setRequired(true)))
  .addSubcommand(s => s.setName('viewprompt').setDescription('View the current custom AI prompt'))
  .addSubcommand(s => s.setName('resetprompt').setDescription('Reset the AI prompt to default'))
  .addSubcommand(s => s.setName('clearmemory').setDescription("Clear the AI's remembered conversation history")
    .addUserOption(o => o.setName('user').setDescription('Clear memory for a specific user only (omit to clear everyone)'))));

cmd(new SlashCommandBuilder().setName('announcement').setDescription('Send a formatted announcement')
  .addChannelOption(o => o.setName('channel').setDescription('Channel to post in').setRequired(true))
  .addStringOption(o => o.setName('title').setDescription('Announcement title').setRequired(true))
  .addStringOption(o => o.setName('message').setDescription('Announcement content').setRequired(true))
  .addStringOption(o => o.setName('ping').setDescription('Who to ping').addChoices({ name: 'everyone', value: 'everyone' }, { name: 'here', value: 'here' }, { name: 'none', value: 'none' }))
  .addStringOption(o => o.setName('banner').setDescription('Banner image URL')));

// ---------------------------------------------------------------------------
// COMMAND METADATA FOR /help (category + min level)
// ---------------------------------------------------------------------------
const HELP_CATEGORIES = {
  'SUPER ADMIN': ['superadmin', 'botconfig', 'aiconfig'],
  'SECURITY': ['antinuke', 'antispam', 'badwords'],
  'MODERATION': ['ban', 'kick', 'timeout', 'warn', 'warnings', 'clearwarns', 'purge', 'lock', 'unlock', 'slowmode'],
  'TICKETS': ['ticket'],
  'WELCOME & GOODBYE': ['welcome', 'goodbye'],
  'DM SYSTEM': ['dm', 'dmlogs'],
  'INVITES': ['invites', 'inviteleaderboard', 'resetinvites'],
  'UTILITY & TOOLS': ['customcommand', 'giveaway', 'statusmonitor', 'weather', 'qrcode', 'remindme', 'poll', 'afk'],
  'INFORMATION': ['serverinfo', 'userinfo', 'roleinfo', 'avatar', 'banner', 'membercount', 'ping', 'stats', 'help', 'pingstatus'],
  'SERVER MANAGEMENT': ['autorole', 'stickyroles', 'addrole', 'removerole', 'verifyconfig', 'verify', 'serverstats', 'extraowner', 'application', 'announcement'],
  'FUN & ENGAGEMENT': ['starboard', 'reactionrole', 'autopublish'],
};

// ---------------------------------------------------------------------------
// REGISTRATION
// ---------------------------------------------------------------------------
async function registerCommands() {
  const rest = new REST({ version: '10' }).setToken(process.env.DISCORD_TOKEN);
  const body = commands.map(c => c.toJSON());
  const guildId = process.env.GUILD_ID;
  try {
    if (guildId) {
      await rest.put(Routes.applicationGuildCommands(process.env.CLIENT_ID, guildId), { body });
      console.log(`Registered ${body.length} guild commands to ${guildId}.`);
    } else {
      await rest.put(Routes.applicationCommands(process.env.CLIENT_ID), { body });
      console.log(`Registered ${body.length} global commands.`);
    }
  } catch (e) {
    console.error('Command registration failed:', e);
  }
}

// ---------------------------------------------------------------------------
// DURATION PARSER
// ---------------------------------------------------------------------------
function parseDuration(str) {
  const m = /^(\d+)\s*(s|m|h|d|w)$/i.exec(str.trim());
  if (!m) return null;
  const n = parseInt(m[1], 10);
  const mult = { s: 1000, m: 60000, h: 3600000, d: 86400000, w: 604800000 }[m[2].toLowerCase()];
  return n * mult;
}

// ---------------------------------------------------------------------------
// MODERATION HELPERS
// ---------------------------------------------------------------------------
async function tryDM(user, embed) {
  try { await user.send({ embeds: [embed] }); return true; } catch { return false; }
}

// ---------------------------------------------------------------------------
// ANTI-NUKE TRACKING
// ---------------------------------------------------------------------------
const nukeTracker = new Map();

function trackAction(guildId, userId, action) {
  if (!nukeTracker.has(guildId)) nukeTracker.set(guildId, new Map());
  const g = nukeTracker.get(guildId);
  if (!g.has(userId)) g.set(userId, {});
  const arr = g.get(userId)[action] || [];
  arr.push(Date.now());
  g.get(userId)[action] = arr;
  return arr;
}

async function checkAntinuke(guild, executorId, action, entity) {
  const gconf = getGuild(guild.id);
  if (!gconf.antinuke.enabled) return;
  if (isProtected(guild, gconf, executorId)) return;
  if (gconf.antinuke.whitelist.includes(executorId)) return;
  const th = gconf.antinuke.thresholds;
  const windowMs = gconf.antinuke.windowMs;
  const arr = trackAction(guild.id, executorId, action).filter(t => Date.now() - t < windowMs);
  const limit = th[action];
  if (!limit || arr.length < limit) return;

  const member = await guild.members.fetch(executorId).catch(() => null);
  const priorTriggers = gconf.antinuke.logs.filter(l => l.executorId === executorId).length;
  let punished = false;
  let actionDesc = 'Could not act (hierarchy/permissions)';
  if (member && botCanActOn(guild, member)) {
    try {
      const dangerousRoles = member.roles.cache.filter(r =>
        r.permissions.has(PermissionFlagsBits.Administrator) ||
        r.permissions.has(PermissionFlagsBits.ManageGuild) ||
        r.permissions.has(PermissionFlagsBits.ManageChannels) ||
        r.permissions.has(PermissionFlagsBits.ManageRoles) ||
        r.permissions.has(PermissionFlagsBits.BanMembers));
      for (const [, role] of dangerousRoles) {
        await member.roles.remove(role, 'Anti-nuke: dangerous mass action detected').catch(() => {});
      }
      if (priorTriggers >= 1) {
        await member.ban({ reason: 'Anti-nuke: repeat dangerous mass action detected' }).catch(() => {});
        actionDesc = 'Dangerous roles stripped + banned (repeat offender)';
      } else {
        await member.timeout(10 * 60 * 1000, 'Anti-nuke: dangerous mass action detected').catch(() => {});
        actionDesc = 'Dangerous roles stripped + 10m timeout';
        await sendVantixNotice(guild, gconf, { userId: executorId, type: 'Antinuke', action: 'Timeout (10m)', reason: `Dangerous mass action: ${action}` });
      }
      punished = true;
    } catch (e) { /* ignore */ }
  }

  const entry = { action, executorId, ts: Date.now(), punished };
  gconf.antinuke.logs.unshift(entry);
  gconf.antinuke.logs = gconf.antinuke.logs.slice(0, 100);
  saveDB();

  const embed = new EmbedBuilder().setColor(COLORS.error)
    .setTitle('🛡️ Anti-Nuke Triggered')
    .setDescription(`Suspicious activity detected: **${action}**`)
    .addFields(
      { name: 'Executor', value: `<@${executorId}> (${executorId})`, inline: true },
      { name: 'Occurrences', value: `${arr.length} within ${Math.round(windowMs / 1000)}s`, inline: true },
      { name: 'Action Taken', value: actionDesc, inline: false },
    ).setTimestamp();

  if (gconf.antinuke.logChannel) {
    const ch = await guild.channels.fetch(gconf.antinuke.logChannel).catch(() => null);
    if (ch) ch.send({ embeds: [embed] }).catch(() => {});
  }
}

// ---------------------------------------------------------------------------
// ANTI-SPAM TRACKING (Discord-AutoMod style: detect -> block -> escalate)
// ---------------------------------------------------------------------------
const spamTracker = new Map();
const inviteRegex = /(discord\.gg|discord\.com\/invite)\/\S+/i;
const linkRegex = /https?:\/\/\S+/gi;

function vantixNoticeEmbed({ userId, type, action, reason }) {
  return new EmbedBuilder()
    .setColor(COLORS.warning)
    .setTitle('Vantix Nodes')
    .addFields(
      { name: 'User', value: `<@${userId}>`, inline: true },
      { name: 'Type', value: type, inline: true },
      { name: 'Action', value: action, inline: true },
      { name: 'Reason', value: reason || 'No reason provided', inline: false },
    )
    .setTimestamp();
}

async function sendVantixNotice(guild, gconf, { userId, type, action, reason }, currentChannel = null) {
  const embed = vantixNoticeEmbed({ userId, type, action, reason });
  if (currentChannel) {
    currentChannel.send({ embeds: [embed] }).catch(() => {});
  }
  if (gconf.antispam.logChannel) {
    const ch = await guild.channels.fetch(gconf.antispam.logChannel).catch(() => null);
    if (ch && ch.id !== currentChannel?.id) ch.send({ embeds: [embed] }).catch(() => {});
  }
  await logEvent(guild, gconf, embed);
}

async function applyEscalation(message, gconf, violation, offenseKey, member) {
  await message.delete().catch(() => {});

  if (!gconf.warnings[message.author.id]) gconf.warnings[message.author.id] = [];
  const priorOffenses = gconf.warnings[message.author.id].filter(w => w.reason.startsWith(offenseKey)).length;
  const reasonText = `${offenseKey} ${violation}`;
  gconf.warnings[message.author.id].push({ reason: reasonText, mod: 'AutoMod', ts: Date.now() });
  saveDB();

  const noticeType = offenseKey === '[AutoMod-BadWord]' ? 'Blockword' : 'Automod';
  let actionTaken = 'Deleted + Warned';
  try {
    if (priorOffenses === 0) {
      await member?.timeout(5 * 60 * 1000, reasonText).catch(() => {});
      actionTaken = 'Deleted + 5m Timeout';
    } else if (priorOffenses === 1) {
      await member?.timeout(30 * 60 * 1000, reasonText).catch(() => {});
      actionTaken = 'Deleted + 30m Timeout';
    } else if (priorOffenses === 2) {
      await member?.timeout(6 * 60 * 60 * 1000, reasonText).catch(() => {});
      actionTaken = 'Deleted + 6h Timeout';
    } else if (priorOffenses === 3) {
      await member?.timeout(24 * 60 * 60 * 1000, reasonText).catch(() => {});
      actionTaken = 'Deleted + 24h Timeout';
    } else {
      await member?.kick(reasonText).catch(() => {});
      actionTaken = 'Deleted + Kicked';
    }
  } catch (e) { /* missing perms etc */ }

  await sendVantixNotice(message.guild, gconf, { userId: message.author.id, type: noticeType, action: actionTaken, reason: violation }, message.channel);

  const embed = new EmbedBuilder().setColor(COLORS.warning)
    .setTitle('🚨 AutoMod Action')
    .addFields(
      { name: 'User', value: `<@${message.author.id}>`, inline: true },
      { name: 'Violation', value: violation, inline: true },
      { name: 'Action', value: actionTaken, inline: true },
      { name: 'Channel', value: `<#${message.channel.id}>`, inline: true },
    ).setTimestamp();
  if (gconf.antispam.logChannel) {
    const ch = await message.guild.channels.fetch(gconf.antispam.logChannel).catch(() => null);
    if (ch) ch.send({ embeds: [embed] }).catch(() => {});
  }
  await logEvent(message.guild, gconf, embed);
}

async function handleBadWords(message) {
  const gconf = getGuild(message.guild.id);
  if (!gconf.badwords || !gconf.badwords.length) return false;
  if (isProtected(message.guild, gconf, message.author.id)) return false;

  const lower = message.content.toLowerCase();
  const hit = gconf.badwords.some(w => {
    const word = w.toLowerCase();
    const escaped = word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const re = new RegExp(`(^|[^a-z0-9])${escaped}([^a-z0-9]|$)`, 'i');
    return re.test(lower) || lower.includes(word);
  });
  if (!hit) return false;

  const member = message.member;
  await applyEscalation(message, gconf, 'Bad word', '[AutoMod-BadWord]', member);
  return true;
}

async function handleAntispam(message) {
  const gconf = getGuild(message.guild.id);
  if (!gconf.antispam.enabled) return;
  if (isProtected(message.guild, gconf, message.author.id)) return;
  const member = message.member;

  const key = `${message.guild.id}:${message.author.id}`;
  if (!spamTracker.has(key)) spamTracker.set(key, { timestamps: [], lastMsgs: [] });
  const tr = spamTracker.get(key);
  const now = Date.now();
  tr.timestamps.push(now);
  tr.timestamps = tr.timestamps.filter(t => now - t < gconf.antispam.msgWindowMs);
  tr.lastMsgs.push({ content: message.content, ts: now });
  tr.lastMsgs = tr.lastMsgs.filter(m => now - m.ts < gconf.antispam.duplicateWindowMs);

  let violation = null;

  if (tr.timestamps.length > gconf.antispam.msgLimit) violation = 'Message flood';
  else if (tr.lastMsgs.filter(m => m.content === message.content && message.content.length > 0).length >= 3) violation = 'Duplicate messages';
  else if (message.mentions.users.size >= gconf.antispam.mentionLimit) violation = 'Mention spam';
  else if (gconf.antispam.blockInvites && inviteRegex.test(message.content)) violation = 'Discord invite link';
  else {
    const links = message.content.match(linkRegex) || [];
    if (links.length >= gconf.antispam.linkLimit) violation = 'Link spam';
  }
  if (!violation) {
    const letters = message.content.replace(/[^a-zA-Z]/g, '');
    if (letters.length >= 8) {
      const caps = letters.replace(/[^A-Z]/g, '').length;
      if ((caps / letters.length) * 100 >= gconf.antispam.capsPercent) violation = 'Excessive caps';
    }
  }
  if (!violation) {
    const repeated = /(.)\1{7,}/.exec(message.content);
    if (repeated && repeated[0].length >= gconf.antispam.repeatedCharLimit) violation = 'Repeated characters';
  }

  if (!violation) return;
  if (member?.permissions.has(PermissionFlagsBits.ManageMessages)) return;

  await applyEscalation(message, gconf, violation, '[AutoMod-Spam]', member);
}

// ---------------------------------------------------------------------------
// HELP MENU BUILDER
// ---------------------------------------------------------------------------
function buildHelpPages() {
  const cats = Object.entries(HELP_CATEGORIES);
  const perPage = Math.ceil(cats.length / 2);
  const pages = [];
  for (let i = 0; i < 2; i++) {
    const slice = cats.slice(i * perPage, (i + 1) * perPage);
    const embed = new EmbedBuilder()
      .setColor(COLORS.info)
      .setTitle('📖 All-in-One Discord Bot — Help Menu')
      .setFooter({ text: `All-in-One Discord Bot • Page ${i + 1}/2 • ${commands.length} commands registered` })
      .setTimestamp();
    for (const [cat, list] of slice) {
      embed.addFields({ name: cat, value: list.map(c => `\`/${c}\``).join(', ') });
    }
    pages.push(embed);
  }
  return pages;
}

function helpButtons(page) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('help_prev').setLabel('◀ Page 1').setStyle(ButtonStyle.Secondary).setDisabled(page === 0),
    new ButtonBuilder().setCustomId('help_next').setLabel('Page 2 ▶').setStyle(ButtonStyle.Secondary).setDisabled(page === 1),
  );
}

// ---------------------------------------------------------------------------
// BOTCONFIG PANEL
// ---------------------------------------------------------------------------
function botconfigMenu() {
  const row = new ActionRowBuilder().addComponents(
    new StringSelectMenuBuilder().setCustomId('botconfig_select').setPlaceholder('Select a system to configure').addOptions(
      { label: 'Welcome', value: 'welcome', emoji: '👋' },
      { label: 'Goodbye', value: 'goodbye', emoji: '👋' },
      { label: 'Anti-Spam', value: 'antispam', emoji: '🛡️' },
      { label: 'Anti-Nuke', value: 'antinuke', emoji: '💣' },
      { label: 'Bad Words', value: 'badwords', emoji: '🤬' },
      { label: 'Tickets', value: 'tickets', emoji: '🎫' },
      { label: 'Verification', value: 'verification', emoji: '✅' },
      { label: 'Autorole', value: 'autorole', emoji: '🎭' },
      { label: 'Sticky Roles', value: 'stickyroles', emoji: '📌' },
      { label: 'Starboard', value: 'starboard', emoji: '⭐' },
      { label: 'Reaction Roles', value: 'reactionroles', emoji: '🔘' },
      { label: 'Auto Publish', value: 'autopublish', emoji: '📢' },
      { label: 'Server Statistics', value: 'serverstats', emoji: '📊' },
      { label: 'Logging', value: 'logging', emoji: '📝' },
      { label: 'Applications', value: 'applications', emoji: '📝' },
    ),
  );
  return row;
}

function configSummaryEmbed(gconf, section, guildId) {
  const e = new EmbedBuilder().setColor(COLORS.neutral).setTitle(`⚙️ Configuration — ${section}`).setTimestamp();
  switch (section) {
    case 'welcome': e.setDescription(`Enabled: **${gconf.welcome.enabled}**\nChannel: ${gconf.welcome.channelId ? `<#${gconf.welcome.channelId}>` : 'none'}\nMessage: ${gconf.welcome.message}\nBanner: ${gconf.welcome.banner || 'none'}`); break;
    case 'goodbye': e.setDescription(`Enabled: **${gconf.goodbye.enabled}**\nChannel: ${gconf.goodbye.channelId ? `<#${gconf.goodbye.channelId}>` : 'none'}\nMessage: ${gconf.goodbye.message}\nBanner: ${gconf.goodbye.banner || 'none'}`); break;
    case 'antispam': e.setDescription(`Enabled: **${gconf.antispam.enabled}**\nUse \`/antispam config\` to edit thresholds.\n${JSON.stringify(gconf.antispam, null, 2).slice(0, 900)}`); break;
    case 'antinuke': e.setDescription(`Enabled: **${gconf.antinuke.enabled}**\nUse \`/antinuke config\` to edit thresholds.\n${JSON.stringify(gconf.antinuke.thresholds, null, 2)}`); break;
    case 'badwords': e.setDescription(`${gconf.badwords.length} word(s) configured. Use \`/badwords add|remove|list\`.`); break;
    case 'tickets': {
      const openCount = (ticketDB.countOpenByGuild.get(guildId || '') || { c: 0 }).c;
      e.setDescription(`Category: ${gconf.tickets.categoryId ? `<#${gconf.tickets.categoryId}>` : 'not set'}\nSupport roles: ${gconf.tickets.supportRoles.map(r => `<@&${r}>`).join(', ') || 'none'}\nOpen tickets: ${openCount}`);
      break;
    }
    case 'verification': e.setDescription(`Enabled: **${gconf.verification.enabled}**\nRole: ${gconf.verification.roleId ? `<@&${gconf.verification.roleId}>` : 'none'}\nChannel: ${gconf.verification.channelId ? `<#${gconf.verification.channelId}>` : 'none'}`); break;
    case 'autorole': e.setDescription(`Role: ${gconf.autorole.roleId ? `<@&${gconf.autorole.roleId}>` : 'none set'}\nUse \`/autorole set|remove\`.`); break;
    case 'stickyroles': e.setDescription(`Enabled: **${gconf.stickyroles.enabled}**`); break;
    case 'starboard': e.setDescription(`Enabled: **${gconf.starboard.enabled}**\nChannel: ${gconf.starboard.channelId ? `<#${gconf.starboard.channelId}>` : 'none'}\nThreshold: ${gconf.starboard.threshold}`); break;
    case 'reactionroles': e.setDescription(`${gconf.reactionroles.length} reaction role(s) configured. Use \`/reactionrole add|remove|list\`.`); break;
    case 'autopublish': e.setDescription(`Channels: ${gconf.autopublish.channels.map(c => `<#${c}>`).join(', ') || 'none'}`); break;
    case 'serverstats': e.setDescription(`Channels configured: ${Object.keys(gconf.serverstats.channels).length}. Use \`/serverstats setup|remove\`.`); break;
    case 'logging': e.setDescription(`Log channel: ${gconf.logging.channelId ? `<#${gconf.logging.channelId}>` : 'not set'}`); break;
    case 'applications': {
      const pending = gconf.applications.submissions.filter(s => s.status === 'pending').length;
      e.setDescription(`Review channel: ${gconf.applications.reviewChannelId ? `<#${gconf.applications.reviewChannelId}>` : 'not set'}\nQuestions: ${gconf.applications.questions.length}\nPending: ${pending}\nUse \`/application\` subcommands to configure.`);
      break;
    }
    default: e.setDescription('Unknown section.');
  }
  return e;
}

// ---------------------------------------------------------------------------
// STATUS MONITOR LOOP
// ---------------------------------------------------------------------------
function checkUrl(url) {
  return new Promise((resolve) => {
    try {
      const req = https.get(url, { timeout: 8000 }, (res) => {
        resolve(res.statusCode < 400);
        res.resume();
      });
      req.on('error', () => resolve(false));
      req.on('timeout', () => { req.destroy(); resolve(false); });
    } catch { resolve(false); }
  });
}

async function statusMonitorTick() {
  for (const [guildId, gconf] of Object.entries(db.guilds)) {
    if (!gconf.statusmonitors?.length) continue;
    const guild = client.guilds.cache.get(guildId);
    if (!guild) continue;
    for (const mon of gconf.statusmonitors) {
      const online = await checkUrl(mon.url);
      const status = online ? 'online' : 'offline';
      const statusChanged = mon.lastStatus !== status;
      mon.lastStatus = status;

      const ch = await guild.channels.fetch(mon.channelId).catch(() => null);
      if (!ch) continue;

      const embed = new EmbedBuilder()
        .setColor(online ? COLORS.success : COLORS.error)
        .setTitle('📡 Status Monitor')
        .addFields(
          { name: 'URL', value: mon.url, inline: false },
          { name: 'Status', value: online ? '🟢 Online' : '🔴 Offline', inline: true },
          { name: 'Ping', value: `${Math.round(client.ws.ping)}ms`, inline: true },
        )
        .setFooter({ text: `Last updated: ${new Date().toLocaleTimeString()}` })
        .setTimestamp();

      if (mon.messageId) {
        const msg = await ch.messages.fetch(mon.messageId).catch(() => null);
        if (msg) {
          await msg.edit({ embeds: [embed] }).catch(() => {});
        } else {
          const sent = await ch.send({ embeds: [embed] }).catch(() => null);
          if (sent) mon.messageId = sent.id;
        }
      } else {
        const sent = await ch.send({ embeds: [embed] }).catch(() => null);
        if (sent) mon.messageId = sent.id;
      }

      if (statusChanged) {
        const alertEmbed = online ? successEmbed(`${mon.url} is back **online**.`, 'Status Update') : errorEmbed(`${mon.url} appears to be **offline**.`, 'Status Update');
        ch.send({ embeds: [alertEmbed] }).catch(() => {});
      }
    }
  }
  saveDB();
}
setInterval(statusMonitorTick, 60 * 1000);

// ---------------------------------------------------------------------------
// LIVE PING STATUS MESSAGE (edits every minute)
// ---------------------------------------------------------------------------
const pingStatusMessages = new Map();

async function pingStatusTick() {
  for (const [guildId, info] of pingStatusMessages.entries()) {
    const guild = client.guilds.cache.get(guildId);
    if (!guild) continue;
    const ch = await guild.channels.fetch(info.channelId).catch(() => null);
    if (!ch) continue;
    const embed = infoEmbed(`🏓 Bot Ping: **${Math.round(client.ws.ping)}ms**\nLast updated: <t:${Math.floor(Date.now() / 1000)}:R>`, '📡 Live Status');
    if (info.messageId) {
      const msg = await ch.messages.fetch(info.messageId).catch(() => null);
      if (msg) { await msg.edit({ embeds: [embed] }).catch(() => {}); continue; }
    }
    const sent = await ch.send({ embeds: [embed] }).catch(() => null);
    if (sent) info.messageId = sent.id;
  }
}
setInterval(pingStatusTick, 60 * 1000);

// ---------------------------------------------------------------------------
// REMINDER LOOP
// ---------------------------------------------------------------------------
async function reminderTick() {
  const now = Date.now();
  for (const gconf of Object.values(db.guilds)) {
    if (!gconf.reminders?.length) continue;
    const due = gconf.reminders.filter(r => r.remindAt <= now);
    if (!due.length) continue;
    gconf.reminders = gconf.reminders.filter(r => r.remindAt > now);
    saveDB();
    for (const r of due) {
      const user = await client.users.fetch(r.userId).catch(() => null);
      if (!user) continue;
      const embed = infoEmbed(r.text, '⏰ Reminder');
      const ok = await tryDM(user, embed);
      if (!ok) {
        const ch = await client.channels.fetch(r.channelId).catch(() => null);
        if (ch) ch.send({ content: `<@${r.userId}>`, embeds: [embed] }).catch(() => {});
      }
    }
  }
}
setInterval(reminderTick, 15000);

// ---------------------------------------------------------------------------
// GIVEAWAY LOOP
// ---------------------------------------------------------------------------
async function endGiveaway(guild, messageId, gconf, forcedWinners = null) {
  const g = gconf.giveaways[messageId];
  if (!g || g.ended) return;
  g.ended = true;
  const channel = await guild.channels.fetch(g.channelId).catch(() => null);
  let winners = [];
  if (forcedWinners) {
    winners = forcedWinners;
  } else {
    const pool = [...g.entrants];
    for (let i = 0; i < g.winners && pool.length; i++) {
      const idx = Math.floor(Math.random() * pool.length);
      winners.push(pool.splice(idx, 1)[0]);
    }
  }
  saveDB();
  if (channel) {
    const embed = new EmbedBuilder().setColor(COLORS.success).setTitle('🎉 Giveaway Ended')
      .setDescription(winners.length ? `Prize: **${g.prize}**\nWinner(s): ${winners.map(w => `<@${w}>`).join(', ')}` : `Prize: **${g.prize}**\nNo valid entrants — no winner.`)
      .setTimestamp();
    channel.send({ embeds: [embed] }).catch(() => {});
    const msg = await channel.messages.fetch(messageId).catch(() => null);
    if (msg) {
      const disabledRow = new ActionRowBuilder().addComponents(new ButtonBuilder().setCustomId('giveaway_ended').setLabel('🎉 Ended').setStyle(ButtonStyle.Secondary).setDisabled(true));
      msg.edit({ components: [disabledRow] }).catch(() => {});
    }
  }
  return winners;
}

async function giveawayTick() {
  const now = Date.now();
  for (const [guildId, gconf] of Object.entries(db.guilds)) {
    if (!gconf.giveaways) continue;
    const guild = client.guilds.cache.get(guildId);
    if (!guild) continue;
    for (const [msgId, g] of Object.entries(gconf.giveaways)) {
      if (!g.ended && g.endsAt <= now) {
        await endGiveaway(guild, msgId, gconf);
      }
    }
  }
}
setInterval(giveawayTick, 15000);

// ---------------------------------------------------------------------------
// SERVER STATS UPDATE LOOP
// ---------------------------------------------------------------------------
async function serverStatsTick() {
  for (const [guildId, gconf] of Object.entries(db.guilds)) {
    const chans = gconf.serverstats?.channels;
    if (!chans || !Object.keys(chans).length) continue;
    const guild = client.guilds.cache.get(guildId);
    if (!guild) continue;
    await guild.members.fetch().catch(() => {});
    const members = guild.members.cache;
    const bots = members.filter(m => m.user.bot).size;
    const humans = members.size - bots;
    const online = members.filter(m => m.presence && m.presence.status !== 'offline').size;
    const values = {
      members: `Members: ${guild.memberCount}`,
      bots: `Bots: ${bots}`,
      humans: `Humans: ${humans}`,
      online: `Online: ${online}`,
      boosts: `Boosts: ${guild.premiumSubscriptionCount || 0}`,
    };
    for (const [key, chId] of Object.entries(chans)) {
      const ch = guild.channels.cache.get(chId);
      if (ch && values[key] && ch.name !== values[key]) {
        ch.setName(values[key]).catch(() => {});
      }
    }
  }
}
setInterval(serverStatsTick, 10 * 60 * 1000);

// ---------------------------------------------------------------------------
// EVENT: READY (fires under 'ready' on this discord.js version; the alias
// 'clientReady' is used in newer versions, so both are handled below).
// ---------------------------------------------------------------------------
let readyHandled = false;
async function handleClientReady() {
  if (readyHandled) return;
  readyHandled = true;
  console.log(`Logged in as ${client.user.tag}`);
  await registerCommands();
  client.user.setPresence({ activities: [{ name: '/help' }], status: 'online' });
}
client.once('ready', handleClientReady);
client.once('clientReady', handleClientReady);

// ---------------------------------------------------------------------------
// EVENT: GUILD MEMBER ADD
// ---------------------------------------------------------------------------
const joinTracker = new Map();

client.on('guildMemberAdd', async (member) => {
  const gconf = getGuild(member.guild.id);

  const arr = joinTracker.get(member.guild.id) || [];
  arr.push(Date.now());
  const recent = arr.filter(t => Date.now() - t < 10000);
  joinTracker.set(member.guild.id, recent);
  if (gconf.antinuke.enabled && recent.length >= 10) {
    const embed = warnEmbed(`Rapid join detected: ${recent.length} joins in 10s. Consider enabling verification or lockdown.`, 'Possible Raid');
    if (gconf.antinuke.logChannel) {
      const ch = await member.guild.channels.fetch(gconf.antinuke.logChannel).catch(() => null);
      if (ch) ch.send({ embeds: [embed] }).catch(() => {});
    }
  }

  if (gconf.autorole.roleId) {
    const role = member.guild.roles.cache.get(gconf.autorole.roleId);
    if (role) await member.roles.add(role).catch(() => {});
  }

  if (gconf.stickyroles.enabled && gconf.stickyroles.store[member.id]) {
    const roleIds = gconf.stickyroles.store[member.id].filter(id => member.guild.roles.cache.has(id));
    if (roleIds.length) await member.roles.add(roleIds).catch(() => {});
  }

  try {
    const newInvites = await member.guild.invites.fetch();
    const cache = gconf.invites.cache;
    let usedCode = null;
    for (const [code, invite] of newInvites) {
      const prev = cache[code] || 0;
      if (invite.uses > prev) { usedCode = code; }
      cache[code] = invite.uses;
    }
    if (usedCode) {
      const invite = newInvites.get(usedCode);
      const inviterId = invite.inviter?.id;
      gconf.invites.joins[member.id] = { code: usedCode, inviter: inviterId };
      if (inviterId) gconf.invites.leaders[inviterId] = (gconf.invites.leaders[inviterId] || 0) + 1;
    }
    saveDB();
  } catch (e) { /* missing ManageGuild permission */ }

  if (gconf.welcome.enabled && gconf.welcome.channelId) {
    const ch = await member.guild.channels.fetch(gconf.welcome.channelId).catch(() => null);
    if (ch) {
      const text = replaceVars(gconf.welcome.message, { user: member.user, guild: member.guild });
      if (gconf.welcome.embed) {
        const embed = new EmbedBuilder().setColor(COLORS.success).setTitle('👋 Welcome!').setDescription(text)
          .setThumbnail(member.user.displayAvatarURL()).setTimestamp();
        if (gconf.welcome.banner) embed.setImage(gconf.welcome.banner);
        ch.send({ embeds: [embed] }).catch(() => {});
      } else {
        ch.send({ content: text }).catch(() => {});
      }
    }
  }
});

// ---------------------------------------------------------------------------
// EVENT: GUILD MEMBER REMOVE
// ---------------------------------------------------------------------------
client.on('guildMemberRemove', async (member) => {
  const gconf = getGuild(member.guild.id);

  if (gconf.stickyroles.enabled) {
    const roleIds = member.roles.cache.filter(r => r.id !== member.guild.id).map(r => r.id);
    gconf.stickyroles.store[member.id] = roleIds;
    saveDB();
  }

  if (gconf.goodbye.enabled && gconf.goodbye.channelId) {
    const ch = await member.guild.channels.fetch(gconf.goodbye.channelId).catch(() => null);
    if (ch) {
      const text = replaceVars(gconf.goodbye.message, { user: member.user, guild: member.guild });
      if (gconf.goodbye.embed) {
        const embed = new EmbedBuilder().setColor(COLORS.error).setTitle('👋 Goodbye').setDescription(text)
          .setThumbnail(member.user.displayAvatarURL()).setTimestamp();
        if (gconf.goodbye.banner) embed.setImage(gconf.goodbye.banner);
        ch.send({ embeds: [embed] }).catch(() => {});
      } else {
        ch.send({ content: text }).catch(() => {});
      }
    }
  }

  const log = await member.guild.fetchAuditLogs({ type: 20, limit: 1 }).catch(() => null);
  const entry = log?.entries.first();
  if (entry && entry.target?.id === member.id && Date.now() - entry.createdTimestamp < 5000) {
    await checkAntinuke(member.guild, entry.executor.id, 'kick');
  }
});

// ---------------------------------------------------------------------------
// EVENT: MESSAGE CREATE
// ---------------------------------------------------------------------------
client.on('messageCreate', async (message) => {
  if (!message.guild || message.author.bot) return;
  const gconf = getGuild(message.guild.id);

  const ticketRow = ticketDB.getOpenByChannel.get(message.channel.id);
  if (ticketRow) {
    const attachments = message.attachments.size ? [...message.attachments.values()].map(a => a.url).join(' ') : null;
    ticketDB.addTranscriptLine.run(message.channel.id, message.guild.id, message.author.tag, message.content || '', attachments, Date.now());
  }

  if (gconf.afk[message.author.id]) {
    delete gconf.afk[message.author.id];
    saveDB();
    message.reply({ embeds: [infoEmbed('Welcome back — I removed your AFK status.', '👋 AFK Removed')] }).then(m => setTimeout(() => m.delete().catch(() => {}), 5000)).catch(() => {});
  }
  for (const [, user] of message.mentions.users) {
    if (gconf.afk[user.id]) {
      message.reply({ embeds: [infoEmbed(`${user.username} is AFK: ${gconf.afk[user.id].reason}`, '💤 AFK')] }).catch(() => {});
    }
  }

  if (message.content.startsWith('!')) {
    const name = message.content.slice(1).split(' ')[0].toLowerCase();
    if (gconf.customcommands[name]) {
      message.channel.send({ content: gconf.customcommands[name] }).catch(() => {});
      return;
    }
  }

  if (message.mentions.has(client.user)) {
    await handleAIChat(message).catch(e => console.error('AI chat error:', e));
    return;
  }

  const wordBlocked = await handleBadWords(message).catch(e => { console.error('badwords error:', e); return false; });
  if (wordBlocked) return;
  await handleAntispam(message).catch(e => console.error('antispam error:', e));

  if (message.channel.type === ChannelType.GuildAnnouncement && gconf.autopublish.channels.includes(message.channel.id)) {
    message.crosspost().catch(() => {});
  }
});

// ---------------------------------------------------------------------------
// AI CHAT (OpenRouter) — "Jarvis"
// ---------------------------------------------------------------------------
async function callOpenRouter(messages) {
  const apiKey = process.env.OPENROUTER_API_KEY;
  if (!apiKey) return { error: 'AI chat is not configured (missing OPENROUTER_API_KEY).' };
  const model = process.env.OPENROUTER_MODEL || 'openai/gpt-4o-mini';
  try {
    const res = await fetch('https://openrouter.ai/api/v1/chat/completions', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ model, messages, temperature: 0.4 }),
    });
    const data = await res.json();
    if (!res.ok) return { error: data?.error?.message || 'AI request failed.' };
    const content = data?.choices?.[0]?.message?.content;
    if (!content) return { error: 'AI returned an empty response.' };
    return { content };
  } catch (e) {
    return { error: 'Could not reach the AI service.' };
  }
}

function aiRememberFact(gconf, userId, fact) {
  if (!gconf.ai.memory[userId]) gconf.ai.memory[userId] = [];
  gconf.ai.memory[userId].push({ role: 'fact', content: fact, ts: Date.now() });
  gconf.ai.memory[userId] = gconf.ai.memory[userId].slice(-20);
  saveDB();
}

function aiRememberExchange(gconf, userId, userText, assistantText) {
  if (!gconf.ai.memory[userId]) gconf.ai.memory[userId] = [];
  gconf.ai.memory[userId].push({ role: 'user', content: userText, ts: Date.now() });
  gconf.ai.memory[userId].push({ role: 'assistant', content: assistantText, ts: Date.now() });
  gconf.ai.memory[userId] = gconf.ai.memory[userId].slice(-20);
  saveDB();
}

function fakeInteraction(message) {
  return { guild: message.guild, member: message.member };
}

async function handleAIChat(message) {
  const gconf = getGuild(message.guild.id);
  const cleaned = message.content.replace(/<@!?\d+>/g, '').trim();
  const targetUser = message.mentions.users.find(u => u.id !== client.user.id);

  const basePrompt = [
    'You are Jarvis, the AI assistant built into a Discord bot called VantixNodes.',
    "You can chat normally like a helpful assistant, AND you can perform actions in this Discord server on the user's behalf when clearly asked.",
    'Available actions (reply with ONLY raw JSON, nothing else, when performing one):',
    '{"action":"timeout","targetId":"<user id>","minutes":<int>,"reason":"<text>"}',
    '{"action":"kick","targetId":"<user id>","reason":"<text>"}',
    '{"action":"ban","targetId":"<user id>","reason":"<text>"}',
    '{"action":"warn","targetId":"<user id>","reason":"<text>"}',
    '{"action":"purge","amount":<int 1-100>}',
    '{"action":"lock"}',
    '{"action":"unlock"}',
    '{"action":"slowmode","seconds":<int 0-21600>}',
    '{"action":"addrole","targetId":"<user id>","roleId":"<role id>"}',
    '{"action":"removerole","targetId":"<user id>","roleId":"<role id>"}',
    '{"action":"announcement","channelId":"<channel id, default to this channel if unspecified>","title":"<text>","message":"<text>"}',
    '{"action":"remember","fact":"<short fact about the user to remember for later, in your own words>"}',
    'Only use "addrole"/"removerole" if a role was clearly identified (e.g. mentioned as <@&ID> or you were told its ID) — never guess a role ID.',
    'Minutes/seconds should come from what the user wrote (e.g. "10m" -> 10, "1h" -> 60); pick a sensible default if unclear.',
    'For every other message — normal conversation, questions, or when no action clearly applies — reply normally in plain, friendly text and NEVER wrap it in JSON.',
    targetUser ? `The message mentions this user ID (besides you): ${targetUser.id}` : 'No other user was mentioned in this message.',
    `This channel's ID is ${message.channel.id}.`,
  ].join('\n');

  const customPrompt = gconf.ai.customPrompt ? `\n\nAdditional instructions set by this server's Super Admins (follow these too):\n${gconf.ai.customPrompt}` : '';

  const memory = gconf.ai.memory[message.author.id] || [];
  const memoryLines = memory.slice(-10).map(m => {
    if (m.role === 'fact') return `[Remembered fact about this user]: ${m.content}`;
    return `[${m.role === 'user' ? 'User previously said' : 'You previously replied'}]: ${m.content}`;
  }).join('\n');
  const memoryBlock = memoryLines ? `\n\nWhat you remember about this user from earlier conversations:\n${memoryLines}` : '';

  const systemPrompt = basePrompt + customPrompt + memoryBlock;

  const result = await callOpenRouter([
    { role: 'system', content: systemPrompt },
    { role: 'user', content: cleaned || 'Hello' },
  ]);

  if (result.error) {
    return message.reply({ embeds: [errorEmbed(result.error)] }).catch(() => {});
  }

  let parsed = null;
  try { parsed = JSON.parse(result.content.trim()); } catch { /* not JSON, treat as normal chat */ }

  if (parsed && parsed.action) {
    const summary = await executeAIAction(message, gconf, parsed);
    aiRememberExchange(gconf, message.author.id, cleaned, summary || `(performed action: ${parsed.action})`);
    return;
  }

  const replyText = result.content.slice(0, 1900);
  aiRememberExchange(gconf, message.author.id, cleaned, replyText);
  return message.reply({ content: replyText }).catch(() => {});
}

async function executeAIAction(message, gconf, parsed) {
  const guild = message.guild;
  const fakeInt = fakeInteraction(message);
  const hasPerm = (flag) => message.member.permissions.has(flag);
  const bail = async (text) => { await message.reply({ embeds: [errorEmbed(text)] }).catch(() => {}); return text; };

  switch (parsed.action) {
    case 'timeout':
    case 'kick':
    case 'ban':
    case 'warn': {
      const targetId = parsed.targetId;
      if (!targetId) return bail('The AI did not identify a valid target user.');
      const permMap = { timeout: PermissionFlagsBits.ModerateMembers, kick: PermissionFlagsBits.KickMembers, ban: PermissionFlagsBits.BanMembers, warn: PermissionFlagsBits.ModerateMembers };
      if (!hasPerm(permMap[parsed.action]) && !requireLevel(fakeInt, gconf, LEVEL.MOD)) return bail('You do not have permission to do that.');
      if (isProtected(guild, gconf, targetId)) return bail('That user is protected and cannot be moderated.');
      const targetMember = await guild.members.fetch(targetId).catch(() => null);
      if (!targetMember) return bail('I could not find that member in this server.');
      if (!actorOutranks(message.member, targetMember, guild, gconf) && !isBotOwner(message.author.id)) return bail('You cannot moderate someone with an equal or higher role.');
      const reason = (parsed.reason || 'No reason provided').slice(0, 400);

      if (parsed.action === 'warn') {
        if (!gconf.warnings[targetId]) gconf.warnings[targetId] = [];
        gconf.warnings[targetId].push({ reason, mod: message.author.id, ts: Date.now() });
        saveDB();
        await tryDM(targetMember.user, warnEmbed(`You were warned in **${guild.name}**: ${reason}`));
        const embed = successEmbed(`${targetMember} has been warned.\nReason: ${reason}`, '🤖 Jarvis Action');
        await logEvent(guild, gconf, embed);
        await sendVantixNotice(guild, gconf, { userId: targetId, type: 'AI Chat', action: 'Warn', reason }, message.channel);
        await message.reply({ embeds: [embed] }).catch(() => {});
        return `Warned ${targetMember.user.tag}: ${reason}`;
      }
      if (!botCanActOn(guild, targetMember)) return bail("I don't have a high enough role to do that.");

      if (parsed.action === 'timeout') {
        const minutes = Math.max(1, Math.min(parseInt(parsed.minutes, 10) || 10, 40320));
        await targetMember.timeout(minutes * 60 * 1000, reason).catch(() => {});
        await tryDM(targetMember.user, warnEmbed(`You were timed out in **${guild.name}** for ${minutes}m: ${reason}`));
        const embed = successEmbed(`${targetMember} has been timed out for ${minutes}m.\nReason: ${reason}`, '🤖 Jarvis Action');
        await logEvent(guild, gconf, embed);
        await sendVantixNotice(guild, gconf, { userId: targetId, type: 'AI Chat', action: `Timeout (${minutes}m)`, reason }, message.channel);
        await message.reply({ embeds: [embed] }).catch(() => {});
        return `Timed out ${targetMember.user.tag} for ${minutes}m: ${reason}`;
      }
      if (parsed.action === 'kick') {
        await tryDM(targetMember.user, warnEmbed(`You were kicked from **${guild.name}**: ${reason}`));
        await targetMember.kick(reason).catch(() => {});
        const embed = successEmbed(`${targetMember} has been kicked.\nReason: ${reason}`, '🤖 Jarvis Action');
        await logEvent(guild, gconf, embed);
        await sendVantixNotice(guild, gconf, { userId: targetId, type: 'AI Chat', action: 'Kick', reason }, message.channel);
        await message.reply({ embeds: [embed] }).catch(() => {});
        return `Kicked ${targetMember.user.tag}: ${reason}`;
      }
      if (parsed.action === 'ban') {
        await tryDM(targetMember.user, warnEmbed(`You were banned from **${guild.name}**: ${reason}`));
        await guild.members.ban(targetId, { reason }).catch(() => {});
        const embed = successEmbed(`${targetMember} has been banned.\nReason: ${reason}`, '🤖 Jarvis Action');
        await logEvent(guild, gconf, embed);
        await sendVantixNotice(guild, gconf, { userId: targetId, type: 'AI Chat', action: 'Ban', reason }, message.channel);
        await message.reply({ embeds: [embed] }).catch(() => {});
        return `Banned ${targetMember.user.tag}: ${reason}`;
      }
      break;
    }
    case 'purge': {
      if (!hasPerm(PermissionFlagsBits.ManageMessages) && !isBotSuperAdmin(fakeInt, gconf)) return bail('You do not have permission to purge messages.');
      const amount = Math.max(1, Math.min(parseInt(parsed.amount, 10) || 10, 100));
      const deleted = await message.channel.bulkDelete(amount, true).catch(() => null);
      if (!deleted) return bail('Could not delete messages (they may be older than 14 days).');
      const embed = successEmbed(`Deleted ${deleted.size} messages.`, '🤖 Jarvis Action');
      await logEvent(guild, gconf, embed);
      await message.channel.send({ embeds: [embed] }).catch(() => {});
      return `Purged ${deleted.size} messages.`;
    }
    case 'lock':
    case 'unlock': {
      if (!hasPerm(PermissionFlagsBits.ManageChannels) && !isBotSuperAdmin(fakeInt, gconf)) return bail('You do not have permission to do that.');
      await message.channel.permissionOverwrites.edit(guild.roles.everyone, { SendMessages: parsed.action === 'lock' ? false : null });
      await message.reply({ embeds: [successEmbed(`Channel ${parsed.action === 'lock' ? 'locked' : 'unlocked'}.`)] }).catch(() => {});
      return `Channel ${parsed.action === 'lock' ? 'locked' : 'unlocked'}.`;
    }
    case 'slowmode': {
      if (!hasPerm(PermissionFlagsBits.ManageChannels) && !isBotSuperAdmin(fakeInt, gconf)) return bail('You do not have permission to do that.');
      const seconds = Math.max(0, Math.min(parseInt(parsed.seconds, 10) || 0, 21600));
      await message.channel.setRateLimitPerUser(seconds).catch(() => {});
      await message.reply({ embeds: [successEmbed(`Slowmode set to ${seconds}s.`)] }).catch(() => {});
      return `Set slowmode to ${seconds}s.`;
    }
    case 'addrole':
    case 'removerole': {
      if (!hasPerm(PermissionFlagsBits.ManageRoles) && !isBotSuperAdmin(fakeInt, gconf)) return bail('You do not have permission to manage roles.');
      const targetId = parsed.targetId;
      const roleId = parsed.roleId;
      if (!targetId || !roleId) return bail('The AI did not identify a valid user and role.');
      const targetMember = await guild.members.fetch(targetId).catch(() => null);
      const role = guild.roles.cache.get(roleId);
      if (!targetMember || !role) return bail('Could not find that member or role.');
      if (role.position >= guild.members.me.roles.highest.position) return bail("I can't manage a role positioned above or equal to my highest role.");
      if (parsed.action === 'addrole') await targetMember.roles.add(role).catch(() => {});
      else await targetMember.roles.remove(role).catch(() => {});
      await message.reply({ embeds: [successEmbed(`${role} ${parsed.action === 'addrole' ? 'added to' : 'removed from'} ${targetMember}.`)] }).catch(() => {});
      return `${parsed.action === 'addrole' ? 'Added' : 'Removed'} role ${role.name} ${parsed.action === 'addrole' ? 'to' : 'from'} ${targetMember.user.tag}.`;
    }
    case 'announcement': {
      if (!requireLevel(fakeInt, gconf, LEVEL.ADMIN)) return bail('You do not have permission to send announcements.');
      const channelId = parsed.channelId || message.channel.id;
      const channel = await guild.channels.fetch(channelId).catch(() => null);
      if (!channel) return bail('Could not find that channel.');
      const embed = new EmbedBuilder().setColor(COLORS.info).setTitle(`📢 ${parsed.title || 'Announcement'}`).setDescription(parsed.message || '').setTimestamp()
        .setFooter({ text: `Announcement by ${message.author.tag} (via Jarvis)` });
      await channel.send({ embeds: [embed] }).catch(() => {});
      await message.reply({ embeds: [successEmbed(`Announcement posted in ${channel}.`)] }).catch(() => {});
      return `Posted an announcement in #${channel.name}.`;
    }
    case 'remember': {
      if (parsed.fact) aiRememberFact(gconf, message.author.id, parsed.fact.slice(0, 300));
      await message.reply({ content: `Got it, I'll remember that: ${parsed.fact || ''}` }).catch(() => {});
      return `Remembered: ${parsed.fact}`;
    }
    default:
      await message.reply({ content: "I understood that as an action I don't know how to do yet." }).catch(() => {});
      return 'Unknown action requested.';
  }
}

// ---------------------------------------------------------------------------
// EVENT: MESSAGE REACTION ADD/REMOVE
// ---------------------------------------------------------------------------
client.on('messageReactionAdd', async (reaction, user) => {
  if (user.bot) return;
  try {
    if (reaction.partial) await reaction.fetch();
    if (reaction.message.partial) await reaction.message.fetch();
  } catch { return; }
  const guild = reaction.message.guild;
  if (!guild) return;
  const gconf = getGuild(guild.id);

  const emojiKey = reaction.emoji.id ? `<:${reaction.emoji.name}:${reaction.emoji.id}>` : reaction.emoji.name;
  const rr = gconf.reactionroles.find(r => r.messageId === reaction.message.id && (r.emoji === emojiKey || r.emoji === reaction.emoji.name));
  if (rr) {
    const member = await guild.members.fetch(user.id).catch(() => null);
    if (member) await member.roles.add(rr.roleId).catch(() => {});
  }

  if (gconf.starboard.enabled && ['⭐', '🌟'].includes(reaction.emoji.name)) {
    const count = reaction.count || 1;
    if (count >= gconf.starboard.threshold) {
      const already = gconf.starboard.messages[reaction.message.id];
      const ch = await guild.channels.fetch(gconf.starboard.channelId).catch(() => null);
      if (ch) {
        const embed = new EmbedBuilder().setColor(0xFFD700)
          .setAuthor({ name: reaction.message.author?.tag || 'Unknown', iconURL: reaction.message.author?.displayAvatarURL() })
          .setDescription(reaction.message.content || '*[attachment/embed]*')
          .addFields({ name: 'Jump', value: `[Original message](${reaction.message.url})` })
          .setTimestamp(reaction.message.createdAt);
        if (reaction.message.attachments.size) embed.setImage(reaction.message.attachments.first().url);
        if (already) {
          const sbMsg = await ch.messages.fetch(already).catch(() => null);
          if (sbMsg) sbMsg.edit({ content: `⭐ **${count}** | <#${reaction.message.channel.id}>`, embeds: [embed] }).catch(() => {});
        } else {
          const sent = await ch.send({ content: `⭐ **${count}** | <#${reaction.message.channel.id}>`, embeds: [embed] }).catch(() => null);
          if (sent) { gconf.starboard.messages[reaction.message.id] = sent.id; saveDB(); }
        }
      }
    }
  }
});

client.on('messageReactionRemove', async (reaction) => {
  try {
    if (reaction.partial) await reaction.fetch();
    if (reaction.message.partial) await reaction.message.fetch();
  } catch { return; }
  const guild = reaction.message.guild;
  if (!guild) return;
  const gconf = getGuild(guild.id);
  if (gconf.starboard.enabled && ['⭐', '🌟'].includes(reaction.emoji.name)) {
    const count = reaction.count || 0;
    const already = gconf.starboard.messages[reaction.message.id];
    if (already) {
      const ch = await guild.channels.fetch(gconf.starboard.channelId).catch(() => null);
      const sbMsg = ch ? await ch.messages.fetch(already).catch(() => null) : null;
      if (sbMsg) sbMsg.edit({ content: `⭐ **${count}** | <#${reaction.message.channel.id}>` }).catch(() => {});
    }
  }
});

// ---------------------------------------------------------------------------
// EVENT: ANTI-NUKE TRIGGERS
// ---------------------------------------------------------------------------
client.on('channelDelete', async (channel) => {
  if (!channel.guild) return;
  const log = await channel.guild.fetchAuditLogs({ type: 12, limit: 1 }).catch(() => null);
  const entry = log?.entries.first();
  if (entry && Date.now() - entry.createdTimestamp < 5000) await checkAntinuke(channel.guild, entry.executor.id, 'channelDelete');
});
client.on('channelCreate', async (channel) => {
  if (!channel.guild) return;
  const log = await channel.guild.fetchAuditLogs({ type: 10, limit: 1 }).catch(() => null);
  const entry = log?.entries.first();
  if (entry && Date.now() - entry.createdTimestamp < 5000) await checkAntinuke(channel.guild, entry.executor.id, 'channelCreate');
});
client.on('roleDelete', async (role) => {
  const log = await role.guild.fetchAuditLogs({ type: 32, limit: 1 }).catch(() => null);
  const entry = log?.entries.first();
  if (entry && Date.now() - entry.createdTimestamp < 5000) await checkAntinuke(role.guild, entry.executor.id, 'roleDelete');
});
client.on('roleCreate', async (role) => {
  const log = await role.guild.fetchAuditLogs({ type: 30, limit: 1 }).catch(() => null);
  const entry = log?.entries.first();
  if (entry && Date.now() - entry.createdTimestamp < 5000) await checkAntinuke(role.guild, entry.executor.id, 'roleCreate');
});
client.on('guildBanAdd', async (ban) => {
  const log = await ban.guild.fetchAuditLogs({ type: 22, limit: 1 }).catch(() => null);
  const entry = log?.entries.first();
  if (entry && Date.now() - entry.createdTimestamp < 5000) await checkAntinuke(ban.guild, entry.executor.id, 'ban');
});
client.on('webhooksUpdate', async (channel) => {
  if (!channel.guild) return;
  const log = await channel.guild.fetchAuditLogs({ limit: 1 }).catch(() => null);
  const entry = log?.entries.first();
  if (entry && [50, 51, 52].includes(entry.action) && Date.now() - entry.createdTimestamp < 5000) {
    await checkAntinuke(channel.guild, entry.executor.id, 'webhook');
  }
});

// ---------------------------------------------------------------------------
// INTERACTION HANDLER
// ---------------------------------------------------------------------------
client.on('interactionCreate', async (interaction) => {
  try {
    if (interaction.isChatInputCommand()) return await handleSlash(interaction);
    if (interaction.isButton()) return await handleButton(interaction);
    if (interaction.isStringSelectMenu()) return await handleSelect(interaction);
    if (interaction.isModalSubmit()) return await handleModal(interaction);
  } catch (err) {
    console.error('Interaction error:', err);
    await safeReply(interaction, { embeds: [errorEmbed('Something went wrong handling that interaction.')], flags: MessageFlags.Ephemeral }).catch(() => {});
  }
});

// ---------------------------------------------------------------------------
// SLASH COMMAND HANDLER
// ---------------------------------------------------------------------------
async function handleSlash(interaction) {
  const { commandName, guild } = interaction;
  if (!guild) return safeReply(interaction, { embeds: [errorEmbed('This bot only works in servers.')], flags: MessageFlags.Ephemeral });
  const gconf = getGuild(guild.id);

  switch (commandName) {
    case 'superadmin': {
      if (!isBotOwner(interaction.member.id) && interaction.member.id !== guild.ownerId && !gconf.superAdmins.includes(interaction.member.id)) {
        return safeReply(interaction, { embeds: [errorEmbed('Only the server owner or existing super admins can manage this.')], flags: MessageFlags.Ephemeral });
      }
      const sub = interaction.options.getSubcommand();
      if (sub === 'add') {
        const user = interaction.options.getUser('user');
        if (!gconf.superAdmins.includes(user.id)) gconf.superAdmins.push(user.id);
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed(`${user} is now a Super Admin.`)] });
      }
      if (sub === 'remove') {
        const user = interaction.options.getUser('user');
        gconf.superAdmins = gconf.superAdmins.filter(id => id !== user.id);
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed(`${user} is no longer a Super Admin.`)] });
      }
      if (sub === 'list') {
        return safeReply(interaction, { embeds: [infoEmbed(gconf.superAdmins.length ? gconf.superAdmins.map(id => `<@${id}>`).join('\n') : 'No super admins configured.', 'Super Admins')] });
      }
      break;
    }

    case 'extraowner': {
      if (!requireLevel(interaction, gconf, LEVEL.SUPER_ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Super Admin or higher.')], flags: MessageFlags.Ephemeral });
      const sub = interaction.options.getSubcommand();
      if (sub === 'add') {
        const user = interaction.options.getUser('user');
        if (!gconf.extraOwners.includes(user.id)) gconf.extraOwners.push(user.id);
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed(`${user} is now an Extra Owner (bot-level permissions only).`)] });
      }
      if (sub === 'remove') {
        const user = interaction.options.getUser('user');
        gconf.extraOwners = gconf.extraOwners.filter(id => id !== user.id);
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed(`${user} is no longer an Extra Owner.`)] });
      }
      if (sub === 'list') {
        return safeReply(interaction, { embeds: [infoEmbed(gconf.extraOwners.length ? gconf.extraOwners.map(id => `<@${id}>`).join('\n') : 'No extra owners configured.', 'Extra Owners')] });
      }
      break;
    }

    case 'botconfig': {
      if (!requireLevel(interaction, gconf, LEVEL.ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Administrator or higher.')], flags: MessageFlags.Ephemeral });
      return safeReply(interaction, { embeds: [infoEmbed('Select a system below to view/configure it.', '⚙️ Bot Configuration')], components: [botconfigMenu()] });
    }

    case 'antinuke': {
      if (!requireLevel(interaction, gconf, LEVEL.EXTRA_OWNER)) return safeReply(interaction, { embeds: [errorEmbed('Requires Extra Owner or higher.')], flags: MessageFlags.Ephemeral });
      const group = interaction.options.getSubcommandGroup(false);
      const sub = interaction.options.getSubcommand();
      if (group === 'whitelist') {
        if (sub === 'add') {
          const user = interaction.options.getUser('user');
          if (!gconf.antinuke.whitelist.includes(user.id)) gconf.antinuke.whitelist.push(user.id);
          saveDB();
          return safeReply(interaction, { embeds: [successEmbed(`${user} added to anti-nuke whitelist.`)] });
        }
        if (sub === 'remove') {
          const user = interaction.options.getUser('user');
          gconf.antinuke.whitelist = gconf.antinuke.whitelist.filter(id => id !== user.id);
          saveDB();
          return safeReply(interaction, { embeds: [successEmbed(`${user} removed from anti-nuke whitelist.`)] });
        }
        if (sub === 'list') {
          return safeReply(interaction, { embeds: [infoEmbed(gconf.antinuke.whitelist.length ? gconf.antinuke.whitelist.map(id => `<@${id}>`).join('\n') : 'Whitelist is empty.', 'Anti-Nuke Whitelist')] });
        }
      }
      if (sub === 'enable') { gconf.antinuke.enabled = true; saveDB(); return safeReply(interaction, { embeds: [successEmbed('Anti-nuke enabled.')] }); }
      if (sub === 'disable') { gconf.antinuke.enabled = false; saveDB(); return safeReply(interaction, { embeds: [successEmbed('Anti-nuke disabled.')] }); }
      if (sub === 'config') {
        const setting = interaction.options.getString('setting');
        const value = interaction.options.getString('value');
        if (!setting) {
          const logChText = gconf.antinuke.logChannel ? `<#${gconf.antinuke.logChannel}>` : 'not set';
          const bodyText = '```json\n' + JSON.stringify(gconf.antinuke.thresholds, null, 2) + '\n```\nwindowMs: ' + gconf.antinuke.windowMs + '\nlogChannel: ' + logChText;
          return safeReply(interaction, { embeds: [infoEmbed(bodyText, 'Anti-Nuke Config')] });
        }
        if (setting === 'logChannel') {
          const ch = interaction.options.getString('value')?.replace(/[<#>]/g, '');
          gconf.antinuke.logChannel = ch;
        } else if (setting === 'windowMs') {
          gconf.antinuke.windowMs = parseInt(value, 10) || gconf.antinuke.windowMs;
        } else {
          gconf.antinuke.thresholds[setting] = parseInt(value, 10) || gconf.antinuke.thresholds[setting];
        }
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed(`Updated \`${setting}\`.`)] });
      }
      if (sub === 'logs') {
        const logs = gconf.antinuke.logs.slice(0, 10);
        const desc = logs.length ? logs.map(l => `**${l.action}** by <@${l.executorId}> — ${l.punished ? 'punished' : 'not punished'} — <t:${Math.floor(l.ts / 1000)}:R>`).join('\n') : 'No events logged yet.';
        return safeReply(interaction, { embeds: [infoEmbed(desc, '🛡️ Anti-Nuke Logs')] });
      }
      break;
    }

    case 'antispam': {
      if (!requireLevel(interaction, gconf, LEVEL.ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Administrator or higher.')], flags: MessageFlags.Ephemeral });
      const sub = interaction.options.getSubcommand();
      if (sub === 'enable') { gconf.antispam.enabled = true; saveDB(); return safeReply(interaction, { embeds: [successEmbed('Anti-spam enabled.')] }); }
      if (sub === 'disable') { gconf.antispam.enabled = false; saveDB(); return safeReply(interaction, { embeds: [successEmbed('Anti-spam disabled.')] }); }
      if (sub === 'config') {
        const setting = interaction.options.getString('setting');
        const value = interaction.options.getString('value');
        if (setting === 'logChannel') {
          gconf.antispam.logChannel = value.replace(/[<#>]/g, '');
        } else if (setting === 'blockInvites') {
          gconf.antispam.blockInvites = value.toLowerCase() === 'true';
        } else {
          const n = parseInt(value, 10);
          if (Number.isNaN(n)) return safeReply(interaction, { embeds: [errorEmbed('Value must be a number for this setting.')], flags: MessageFlags.Ephemeral });
          gconf.antispam[setting] = n;
        }
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed(`Updated \`${setting}\` to \`${value}\`.`)] });
      }
      break;
    }

    case 'badwords': {
      if (!requireLevel(interaction, gconf, LEVEL.MOD)) return safeReply(interaction, { embeds: [errorEmbed('Requires Moderator or higher.')], flags: MessageFlags.Ephemeral });
      const sub = interaction.options.getSubcommand();
      if (sub === 'add') {
        const word = interaction.options.getString('word').toLowerCase();
        if (!gconf.badwords.includes(word)) gconf.badwords.push(word);
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed('Word added to the filter.')], flags: MessageFlags.Ephemeral });
      }
      if (sub === 'remove') {
        const word = interaction.options.getString('word').toLowerCase();
        gconf.badwords = gconf.badwords.filter(w => w !== word);
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed('Word removed from the filter.')], flags: MessageFlags.Ephemeral });
      }
      if (sub === 'list') {
        try {
          await interaction.user.send({ embeds: [infoEmbed(gconf.badwords.length ? gconf.badwords.join(', ') : 'No words configured.', 'Bad Word List')] });
          return safeReply(interaction, { embeds: [successEmbed('Sent you a DM with the list.')], flags: MessageFlags.Ephemeral });
        } catch {
          return safeReply(interaction, { embeds: [errorEmbed('I could not DM you the list. Enable DMs from server members.')], flags: MessageFlags.Ephemeral });
        }
      }
      break;
    }

    case 'ban': case 'kick': case 'timeout': case 'warn': {
      const targetUser = interaction.options.getUser('user');
      const reason = interaction.options.getString('reason') || 'No reason provided';
      if (targetUser.id === interaction.user.id) return safeReply(interaction, { embeds: [errorEmbed('You cannot target yourself.')], flags: MessageFlags.Ephemeral });
      if (isProtected(guild, gconf, targetUser.id)) return safeReply(interaction, { embeds: [warnEmbed('This user is protected and cannot be moderated.')], flags: MessageFlags.Ephemeral });

      const targetMember = await guild.members.fetch(targetUser.id).catch(() => null);

      const permMap = { ban: PermissionFlagsBits.BanMembers, kick: PermissionFlagsBits.KickMembers, timeout: PermissionFlagsBits.ModerateMembers, warn: PermissionFlagsBits.ModerateMembers };
      if (!interaction.member.permissions.has(permMap[commandName]) && !requireLevel(interaction, gconf, LEVEL.MOD)) {
        return safeReply(interaction, { embeds: [errorEmbed('You lack permission to use this command.')], flags: MessageFlags.Ephemeral });
      }

      if (targetMember) {
        if (!actorOutranks(interaction.member, targetMember, guild, gconf) && !isBotSuperAdmin(interaction, gconf)) {
          return safeReply(interaction, { embeds: [errorEmbed('You cannot moderate someone with an equal or higher role.')], flags: MessageFlags.Ephemeral });
        }
        if (commandName !== 'warn' && !botCanActOn(guild, targetMember)) {
          return safeReply(interaction, { embeds: [errorEmbed("I don't have a high enough role to do that.")], flags: MessageFlags.Ephemeral });
        }
      }

      if (commandName === 'warn') {
        if (!gconf.warnings[targetUser.id]) gconf.warnings[targetUser.id] = [];
        gconf.warnings[targetUser.id].push({ reason, mod: interaction.user.id, ts: Date.now() });
        saveDB();
        await tryDM(targetUser, warnEmbed(`You were warned in **${guild.name}**: ${reason}`));
        const embed = successEmbed(`${targetUser} has been warned.\nReason: ${reason}`, 'Member Warned');
        await logEvent(guild, gconf, embed);
        return safeReply(interaction, { embeds: [embed] });
      }

      if (commandName === 'timeout') {
        const durMs = parseDuration(interaction.options.getString('duration'));
        if (!durMs || durMs > 28 * 86400000) return safeReply(interaction, { embeds: [errorEmbed('Invalid duration. Use formats like 10m, 1h, 1d (max 28d).')], flags: MessageFlags.Ephemeral });
        if (!targetMember) return safeReply(interaction, { embeds: [errorEmbed('That user is not in this server.')], flags: MessageFlags.Ephemeral });
        await targetMember.timeout(durMs, reason).catch(e => { throw e; });
        await tryDM(targetUser, warnEmbed(`You were timed out in **${guild.name}** for ${interaction.options.getString('duration')}: ${reason}`));
        const embed = successEmbed(`${targetUser} has been timed out for ${interaction.options.getString('duration')}.\nReason: ${reason}`, 'Member Timed Out');
        await logEvent(guild, gconf, embed);
        return safeReply(interaction, { embeds: [embed] });
      }

      if (commandName === 'kick') {
        if (!targetMember) return safeReply(interaction, { embeds: [errorEmbed('That user is not in this server.')], flags: MessageFlags.Ephemeral });
        await tryDM(targetUser, warnEmbed(`You were kicked from **${guild.name}**: ${reason}`));
        await targetMember.kick(reason);
        const embed = successEmbed(`${targetUser} has been kicked.\nReason: ${reason}`, 'Member Kicked');
        await logEvent(guild, gconf, embed);
        return safeReply(interaction, { embeds: [embed] });
      }

      if (commandName === 'ban') {
        await tryDM(targetUser, warnEmbed(`You were banned from **${guild.name}**: ${reason}`));
        await guild.members.ban(targetUser.id, { reason });
        const embed = successEmbed(`${targetUser} has been banned.\nReason: ${reason}`, 'Member Banned');
        await logEvent(guild, gconf, embed);
        return safeReply(interaction, { embeds: [embed] });
      }
      break;
    }

    case 'warnings': {
      const user = interaction.options.getUser('user');
      const list = gconf.warnings[user.id] || [];
      const desc = list.length ? list.map((w, i) => `**${i + 1}.** ${w.reason} — <@${w.mod === 'AutoMod' ? client.user.id : w.mod}> — <t:${Math.floor(w.ts / 1000)}:R>`).join('\n') : 'No warnings on record.';
      return safeReply(interaction, { embeds: [infoEmbed(desc, `Warnings — ${user.tag}`)] });
    }
    case 'clearwarns': {
      if (!requireLevel(interaction, gconf, LEVEL.MOD)) return safeReply(interaction, { embeds: [errorEmbed('Requires Moderator or higher.')], flags: MessageFlags.Ephemeral });
      const user = interaction.options.getUser('user');
      gconf.warnings[user.id] = [];
      saveDB();
      return safeReply(interaction, { embeds: [successEmbed(`Cleared warnings for ${user}.`)] });
    }
    case 'purge': {
      if (!interaction.member.permissions.has(PermissionFlagsBits.ManageMessages) && !isBotSuperAdmin(interaction, gconf)) return safeReply(interaction, { embeds: [errorEmbed('Requires Manage Messages permission.')], flags: MessageFlags.Ephemeral });
      const amount = interaction.options.getInteger('amount');
      await interaction.deferReply({ flags: MessageFlags.Ephemeral });
      const deleted = await interaction.channel.bulkDelete(amount, true).catch(() => null);
      if (!deleted) return safeReply(interaction, { embeds: [errorEmbed('Could not delete messages (they may be older than 14 days).')] });
      const embed = successEmbed(`Deleted ${deleted.size} messages in ${interaction.channel}.`, 'Purge');
      await logEvent(guild, gconf, embed);
      return safeReply(interaction, { embeds: [embed] });
    }
    case 'lock': {
      if (!interaction.member.permissions.has(PermissionFlagsBits.ManageChannels) && !isBotSuperAdmin(interaction, gconf)) return safeReply(interaction, { embeds: [errorEmbed('Requires Manage Channels permission.')], flags: MessageFlags.Ephemeral });
      await interaction.channel.permissionOverwrites.edit(guild.roles.everyone, { SendMessages: false });
      return safeReply(interaction, { embeds: [successEmbed('Channel locked.')] });
    }
    case 'unlock': {
      if (!interaction.member.permissions.has(PermissionFlagsBits.ManageChannels) && !isBotSuperAdmin(interaction, gconf)) return safeReply(interaction, { embeds: [errorEmbed('Requires Manage Channels permission.')], flags: MessageFlags.Ephemeral });
      await interaction.channel.permissionOverwrites.edit(guild.roles.everyone, { SendMessages: null });
      return safeReply(interaction, { embeds: [successEmbed('Channel unlocked.')] });
    }
    case 'slowmode': {
      if (!interaction.member.permissions.has(PermissionFlagsBits.ManageChannels) && !isBotSuperAdmin(interaction, gconf)) return safeReply(interaction, { embeds: [errorEmbed('Requires Manage Channels permission.')], flags: MessageFlags.Ephemeral });
      const secs = interaction.options.getInteger('seconds');
      await interaction.channel.setRateLimitPerUser(secs);
      return safeReply(interaction, { embeds: [successEmbed(`Slowmode set to ${secs}s.`)] });
    }

    case 'ticket': return handleTicketCommand(interaction, gconf);

    case 'welcome': case 'goodbye': {
      if (!requireLevel(interaction, gconf, LEVEL.ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Administrator or higher.')], flags: MessageFlags.Ephemeral });
      const conf = gconf[commandName];
      const sub = interaction.options.getSubcommand();
      if (sub === 'setup') {
        const channel = interaction.options.getChannel('channel');
        const message = interaction.options.getString('message');
        const banner = interaction.options.getString('banner');
        conf.enabled = true; conf.channelId = channel.id;
        if (message) conf.message = message;
        if (banner) conf.banner = banner;
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed(`${commandName} messages configured in ${channel}.`)] });
      }
      if (sub === 'test') {
        if (!conf.channelId) return safeReply(interaction, { embeds: [errorEmbed(`Run \`/${commandName} setup\` first.`)], flags: MessageFlags.Ephemeral });
        const ch = await guild.channels.fetch(conf.channelId).catch(() => null);
        const text = replaceVars(conf.message, { user: interaction.user, guild });
        const testEmbed = new EmbedBuilder().setColor(commandName === 'welcome' ? COLORS.success : COLORS.error).setDescription(text);
        if (conf.banner) testEmbed.setImage(conf.banner);
        if (ch) ch.send({ embeds: [testEmbed] });
        return safeReply(interaction, { embeds: [successEmbed('Test message sent.')], flags: MessageFlags.Ephemeral });
      }
      if (sub === 'disable') {
        conf.enabled = false; saveDB();
        return safeReply(interaction, { embeds: [successEmbed(`${commandName} messages disabled.`)] });
      }
      break;
    }

    case 'dm': {
      if (!requireLevel(interaction, gconf, LEVEL.ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Administrator or higher.')], flags: MessageFlags.Ephemeral });
      const sub = interaction.options.getSubcommand();
      const message = interaction.options.getString('message');
      await interaction.deferReply({ flags: MessageFlags.Ephemeral });
      const embed = infoEmbed(message, `Message from ${guild.name}`);
      if (sub === 'user') {
        const user = interaction.options.getUser('user');
        const ok = await tryDM(user, embed);
        gconf.dmlogs.unshift({ type: 'user', target: user.id, ok, ts: Date.now() }); gconf.dmlogs = gconf.dmlogs.slice(0, 200); saveDB();
        return safeReply(interaction, { embeds: [ok ? successEmbed(`DM sent to ${user}.`) : errorEmbed(`Could not DM ${user} (DMs closed).`)] });
      }
      if (sub === 'role') {
        const role = interaction.options.getRole('role');
        await guild.members.fetch();
        const members = role.members;
        let sent = 0, failed = 0;
        for (const [, m] of members) {
          const ok = await tryDM(m.user, embed);
          if (ok) sent++; else failed++;
          await new Promise(r => setTimeout(r, 800));
        }
        gconf.dmlogs.unshift({ type: 'role', target: role.id, sent, failed, ts: Date.now() }); gconf.dmlogs = gconf.dmlogs.slice(0, 200); saveDB();
        return safeReply(interaction, { embeds: [successEmbed(`Sent to ${sent} member(s), failed for ${failed}.`)] });
      }
      if (sub === 'everyone') {
        await guild.members.fetch();
        const members = guild.members.cache.filter(m => !m.user.bot);
        let sent = 0, failed = 0;
        for (const [, m] of members) {
          const ok = await tryDM(m.user, embed);
          if (ok) sent++; else failed++;
          await new Promise(r => setTimeout(r, 1000));
        }
        gconf.dmlogs.unshift({ type: 'everyone', sent, failed, ts: Date.now() }); gconf.dmlogs = gconf.dmlogs.slice(0, 200); saveDB();
        return safeReply(interaction, { embeds: [successEmbed(`Sent to ${sent} member(s), failed for ${failed}.`)] });
      }
      break;
    }
    case 'dmlogs': {
      if (!requireLevel(interaction, gconf, LEVEL.ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Administrator or higher.')], flags: MessageFlags.Ephemeral });
      const logs = gconf.dmlogs.slice(0, 10);
      const desc = logs.length ? logs.map(l => `**${l.type}** ${l.target ? `→ <@${l.target}>` : ''} ${l.sent !== undefined ? `(${l.sent} sent / ${l.failed} failed)` : (l.ok ? '✅' : '❌')} — <t:${Math.floor(l.ts / 1000)}:R>`).join('\n') : 'No DM activity logged yet.';
      return safeReply(interaction, { embeds: [infoEmbed(desc, 'DM Logs')] });
    }

    case 'invites': {
      const user = interaction.options.getUser('user') || interaction.user;
      const count = gconf.invites.leaders[user.id] || 0;
      return safeReply(interaction, { embeds: [infoEmbed(`${user} has **${count}** invite(s).`, 'Invite Stats')] });
    }
    case 'inviteleaderboard': {
      const entries = Object.entries(gconf.invites.leaders).sort((a, b) => b[1] - a[1]).slice(0, 10);
      const desc = entries.length ? entries.map(([id, n], i) => `**${i + 1}.** <@${id}> — ${n} invite(s)`).join('\n') : 'No invite data yet.';
      return safeReply(interaction, { embeds: [infoEmbed(desc, '🏆 Invite Leaderboard')] });
    }
    case 'resetinvites': {
      if (!requireLevel(interaction, gconf, LEVEL.ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Administrator or higher.')], flags: MessageFlags.Ephemeral });
      gconf.invites = { cache: {}, joins: {}, leaders: {} };
      saveDB();
      return safeReply(interaction, { embeds: [successEmbed('Invite statistics reset.')] });
    }

    case 'customcommand': {
      if (!requireLevel(interaction, gconf, LEVEL.MOD)) return safeReply(interaction, { embeds: [errorEmbed('Requires Moderator or higher.')], flags: MessageFlags.Ephemeral });
      const sub = interaction.options.getSubcommand();
      if (sub === 'add') {
        const name = interaction.options.getString('name').toLowerCase();
        const response = interaction.options.getString('response');
        gconf.customcommands[name] = response;
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed(`Custom command \`!${name}\` added.`)] });
      }
      if (sub === 'remove') {
        const name = interaction.options.getString('name').toLowerCase();
        delete gconf.customcommands[name];
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed(`Custom command \`!${name}\` removed.`)] });
      }
      if (sub === 'list') {
        const names = Object.keys(gconf.customcommands);
        return safeReply(interaction, { embeds: [infoEmbed(names.length ? names.map(n => `\`!${n}\``).join(', ') : 'No custom commands set.', 'Custom Commands')] });
      }
      break;
    }

    case 'giveaway': {
      if (!requireLevel(interaction, gconf, LEVEL.MOD)) return safeReply(interaction, { embeds: [errorEmbed('Requires Moderator or higher.')], flags: MessageFlags.Ephemeral });
      const sub = interaction.options.getSubcommand();
      if (sub === 'start') {
        const prize = interaction.options.getString('prize');
        const durMs = parseDuration(interaction.options.getString('duration'));
        const winners = interaction.options.getInteger('winners');
        if (!durMs) return safeReply(interaction, { embeds: [errorEmbed('Invalid duration format. Use e.g. 10m, 1h, 1d.')], flags: MessageFlags.Ephemeral });
        const endsAt = Date.now() + durMs;
        const embed = new EmbedBuilder().setColor(COLORS.info).setTitle('🎉 Giveaway!')
          .setDescription(`Prize: **${prize}**\nWinners: **${winners}**\nEnds: <t:${Math.floor(endsAt / 1000)}:R>\n\nClick the button below to enter!`)
          .setTimestamp();
        const row = new ActionRowBuilder().addComponents(new ButtonBuilder().setCustomId('giveaway_enter').setLabel('🎉 Enter').setStyle(ButtonStyle.Primary));
        await interaction.reply({ embeds: [embed], components: [row] });
        const msg = await interaction.fetchReply();
        gconf.giveaways[msg.id] = { channelId: interaction.channel.id, prize, winners, endsAt, entrants: [], ended: false };
        saveDB();
        return;
      }
      if (sub === 'end') {
        const id = interaction.options.getString('messageid');
        if (!gconf.giveaways[id]) return safeReply(interaction, { embeds: [errorEmbed('No giveaway found with that message ID.')], flags: MessageFlags.Ephemeral });
        await endGiveaway(guild, id, gconf);
        return safeReply(interaction, { embeds: [successEmbed('Giveaway ended.')], flags: MessageFlags.Ephemeral });
      }
      if (sub === 'reroll') {
        const id = interaction.options.getString('messageid');
        const g = gconf.giveaways[id];
        if (!g) return safeReply(interaction, { embeds: [errorEmbed('No giveaway found with that message ID.')], flags: MessageFlags.Ephemeral });
        if (!g.entrants.length) return safeReply(interaction, { embeds: [errorEmbed('No entrants to reroll from.')], flags: MessageFlags.Ephemeral });
        const winner = g.entrants[Math.floor(Math.random() * g.entrants.length)];
        const ch = await guild.channels.fetch(g.channelId).catch(() => null);
        if (ch) ch.send({ embeds: [successEmbed(`New winner for **${g.prize}**: <@${winner}>`, '🎉 Reroll')] });
        return safeReply(interaction, { embeds: [successEmbed('Rerolled.')], flags: MessageFlags.Ephemeral });
      }
      break;
    }

    case 'statusmonitor': {
      if (!requireLevel(interaction, gconf, LEVEL.ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Administrator or higher.')], flags: MessageFlags.Ephemeral });
      const sub = interaction.options.getSubcommand();
      if (sub === 'add') {
        const url = interaction.options.getString('url');
        if (!/^https?:\/\//i.test(url)) return safeReply(interaction, { embeds: [errorEmbed('URL must start with http:// or https://')], flags: MessageFlags.Ephemeral });
        const channel = interaction.options.getChannel('channel');
        gconf.statusmonitors.push({ url, channelId: channel.id, lastStatus: null, messageId: null });
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed(`Now monitoring ${url}.`)] });
      }
      if (sub === 'remove') {
        const url = interaction.options.getString('url');
        gconf.statusmonitors = gconf.statusmonitors.filter(m => m.url !== url);
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed('Removed from monitoring.')] });
      }
      if (sub === 'list') {
        const desc = gconf.statusmonitors.length ? gconf.statusmonitors.map(m => `${m.url} — ${m.lastStatus || 'unknown'}`).join('\n') : 'No URLs monitored.';
        return safeReply(interaction, { embeds: [infoEmbed(desc, 'Status Monitors')] });
      }
      break;
    }

    case 'weather': {
      await interaction.deferReply();
      const location = interaction.options.getString('location');
      try {
        const geo = await fetch(`https://geocoding-api.open-meteo.com/v1/search?name=${encodeURIComponent(location)}&count=1`).then(r => r.json());
        if (!geo.results || !geo.results.length) return safeReply(interaction, { embeds: [errorEmbed('Location not found.')] });
        const { latitude, longitude, name, country } = geo.results[0];
        const weather = await fetch(`https://api.open-meteo.com/v1/forecast?latitude=${latitude}&longitude=${longitude}&current_weather=true`).then(r => r.json());
        const cw = weather.current_weather;
        const embed = infoEmbed(`Temperature: **${cw.temperature}°C**\nWind: **${cw.windspeed} km/h**\nCode: ${cw.weathercode}`, `🌤️ Weather in ${name}, ${country}`);
        return safeReply(interaction, { embeds: [embed] });
      } catch (e) {
        return safeReply(interaction, { embeds: [errorEmbed('Could not fetch weather right now.')] });
      }
    }

    case 'qrcode': {
      await interaction.deferReply();
      const text = interaction.options.getString('text');
      const url = `https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=${encodeURIComponent(text)}`;
      return safeReply(interaction, { embeds: [new EmbedBuilder().setColor(COLORS.info).setTitle('QR Code').setImage(url)] });
    }

    case 'remindme': {
      const durMs = parseDuration(interaction.options.getString('when'));
      if (!durMs) return safeReply(interaction, { embeds: [errorEmbed('Invalid time format. Use e.g. 10m, 1h, 2d.')], flags: MessageFlags.Ephemeral });
      const text = interaction.options.getString('text');
      gconf.reminders.push({ userId: interaction.user.id, channelId: interaction.channel.id, remindAt: Date.now() + durMs, text, id: Date.now().toString() });
      saveDB();
      return safeReply(interaction, { embeds: [successEmbed(`I'll remind you in ${interaction.options.getString('when')}.`)], flags: MessageFlags.Ephemeral });
    }

    case 'poll': {
      const question = interaction.options.getString('question');
      const optionsStr = interaction.options.getString('options');
      if (optionsStr) {
        const opts = optionsStr.split(',').map(s => s.trim()).filter(Boolean).slice(0, 5);
        const emojis = ['1️⃣', '2️⃣', '3️⃣', '4️⃣', '5️⃣'];
        const embed = new EmbedBuilder().setColor(COLORS.info).setTitle(`📊 ${question}`)
          .setDescription(opts.map((o, i) => `${emojis[i]} ${o}`).join('\n')).setTimestamp();
        await interaction.reply({ embeds: [embed] });
        const msg = await interaction.fetchReply();
        for (let i = 0; i < opts.length; i++) await msg.react(emojis[i]).catch(() => {});
        return;
      }
      const embed = new EmbedBuilder().setColor(COLORS.info).setTitle(`📊 ${question}`).setTimestamp();
      await interaction.reply({ embeds: [embed] });
      const msg = await interaction.fetchReply();
      await msg.react('👍').catch(() => {});
      await msg.react('👎').catch(() => {});
      return;
    }

    case 'afk': {
      const reason = interaction.options.getString('reason') || 'AFK';
      gconf.afk[interaction.user.id] = { reason, since: Date.now() };
      saveDB();
      return safeReply(interaction, { embeds: [successEmbed(`You are now AFK: ${reason}`)] });
    }

    case 'serverinfo': {
      const owner = await guild.fetchOwner().catch(() => null);
      const embed = new EmbedBuilder().setColor(COLORS.info).setTitle(guild.name).setThumbnail(guild.iconURL())
        .addFields(
          { name: 'Server ID', value: guild.id, inline: true },
          { name: 'Owner', value: owner ? `${owner.user.tag}` : 'Unknown', inline: true },
          { name: 'Members', value: `${guild.memberCount}`, inline: true },
          { name: 'Channels', value: `${guild.channels.cache.size}`, inline: true },
          { name: 'Roles', value: `${guild.roles.cache.size}`, inline: true },
          { name: 'Boosts', value: `${guild.premiumSubscriptionCount || 0} (Level ${guild.premiumTier})`, inline: true },
          { name: 'Created', value: `<t:${Math.floor(guild.createdTimestamp / 1000)}:D>`, inline: true },
        ).setTimestamp();
      return safeReply(interaction, { embeds: [embed] });
    }
    case 'userinfo': {
      const user = interaction.options.getUser('user') || interaction.user;
      const member = await guild.members.fetch(user.id).catch(() => null);
      const embed = new EmbedBuilder().setColor(COLORS.info).setTitle(user.tag).setThumbnail(user.displayAvatarURL())
        .addFields(
          { name: 'ID', value: user.id, inline: true },
          { name: 'Account Created', value: `<t:${Math.floor(user.createdTimestamp / 1000)}:D>`, inline: true },
          { name: 'Joined Server', value: member ? `<t:${Math.floor(member.joinedTimestamp / 1000)}:D>` : 'N/A', inline: true },
          { name: 'Roles', value: member ? (member.roles.cache.filter(r => r.id !== guild.id).map(r => `<@&${r.id}>`).join(', ') || 'None') : 'N/A' },
        ).setTimestamp();
      return safeReply(interaction, { embeds: [embed] });
    }
    case 'roleinfo': {
      const role = interaction.options.getRole('role');
      const embed = new EmbedBuilder().setColor(role.color || COLORS.info).setTitle(`Role: ${role.name}`)
        .addFields(
          { name: 'ID', value: role.id, inline: true },
          { name: 'Position', value: `${role.position}`, inline: true },
          { name: 'Color', value: role.hexColor, inline: true },
          { name: 'Members', value: `${role.members.size}`, inline: true },
          { name: 'Key Permissions', value: role.permissions.toArray().slice(0, 10).join(', ') || 'None' },
        ).setTimestamp();
      return safeReply(interaction, { embeds: [embed] });
    }
    case 'avatar': {
      const user = interaction.options.getUser('user') || interaction.user;
      return safeReply(interaction, { embeds: [new EmbedBuilder().setColor(COLORS.info).setTitle(`${user.tag}'s Avatar`).setImage(user.displayAvatarURL({ size: 512 }))] });
    }
    case 'banner': {
      const user = await client.users.fetch((interaction.options.getUser('user') || interaction.user).id, { force: true });
      if (!user.bannerURL()) return safeReply(interaction, { embeds: [errorEmbed('This user has no banner set.')], flags: MessageFlags.Ephemeral });
      return safeReply(interaction, { embeds: [new EmbedBuilder().setColor(COLORS.info).setTitle(`${user.tag}'s Banner`).setImage(user.bannerURL({ size: 512 }))] });
    }
    case 'membercount': {
      const bots = guild.members.cache.filter(m => m.user.bot).size;
      return safeReply(interaction, { embeds: [infoEmbed(`Total: **${guild.memberCount}**\nHumans: **${guild.memberCount - bots}**\nBots: **${bots}**`, 'Member Count')] });
    }
    case 'ping': {
      const sent = await interaction.reply({ embeds: [infoEmbed('Pinging...')], fetchReply: true });
      const rtt = sent.createdTimestamp - interaction.createdTimestamp;
      return interaction.editReply({ embeds: [infoEmbed(`Bot latency: **${rtt}ms**\nAPI latency: **${Math.round(client.ws.ping)}ms**`, '🏓 Pong!')] });
    }
    case 'stats': {
      const mem = process.memoryUsage().heapUsed / 1024 / 1024;
      const embed = infoEmbed(
        `Uptime: <t:${Math.floor(startTime / 1000)}:R>\nMemory: **${mem.toFixed(1)} MB**\nServers: **${client.guilds.cache.size}**\nUsers: **${client.guilds.cache.reduce((a, g) => a + g.memberCount, 0)}**\nChannels: **${client.channels.cache.size}**\nCommands: **${commands.length}**`,
        '📊 Bot Statistics',
      );
      return safeReply(interaction, { embeds: [embed] });
    }
    case 'help': {
      const pages = buildHelpPages();
      return safeReply(interaction, { embeds: [pages[0]], components: [helpButtons(0)] });
    }
    case 'pingstatus': {
      if (!requireLevel(interaction, gconf, LEVEL.ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Administrator or higher.')], flags: MessageFlags.Ephemeral });
      pingStatusMessages.set(guild.id, { channelId: interaction.channel.id, messageId: null });
      await pingStatusTick();
      return safeReply(interaction, { embeds: [successEmbed('Live ping status started — it will update every minute in this channel.')] });
    }

    case 'autorole': {
      if (!requireLevel(interaction, gconf, LEVEL.ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Administrator or higher.')], flags: MessageFlags.Ephemeral });
      const sub = interaction.options.getSubcommand();
      if (sub === 'set') {
        const role = interaction.options.getRole('role');
        gconf.autorole.roleId = role.id; saveDB();
        return safeReply(interaction, { embeds: [successEmbed(`Autorole set to ${role}.`)] });
      }
      gconf.autorole.roleId = null; saveDB();
      return safeReply(interaction, { embeds: [successEmbed('Autorole removed.')] });
    }
    case 'stickyroles': {
      if (!requireLevel(interaction, gconf, LEVEL.ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Administrator or higher.')], flags: MessageFlags.Ephemeral });
      const sub = interaction.options.getSubcommand();
      gconf.stickyroles.enabled = sub === 'enable'; saveDB();
      return safeReply(interaction, { embeds: [successEmbed(`Sticky roles ${sub}d.`)] });
    }
    case 'addrole': case 'removerole': {
      if (!interaction.member.permissions.has(PermissionFlagsBits.ManageRoles) && !isBotSuperAdmin(interaction, gconf)) return safeReply(interaction, { embeds: [errorEmbed('Requires Manage Roles permission.')], flags: MessageFlags.Ephemeral });
      const user = interaction.options.getUser('user');
      const role = interaction.options.getRole('role');
      const member = await guild.members.fetch(user.id).catch(() => null);
      if (!member) return safeReply(interaction, { embeds: [errorEmbed('User not found in this server.')], flags: MessageFlags.Ephemeral });
      if (role.position >= guild.members.me.roles.highest.position) return safeReply(interaction, { embeds: [errorEmbed("I can't manage a role positioned above or equal to my highest role.")], flags: MessageFlags.Ephemeral });
      if (commandName === 'addrole') await member.roles.add(role).catch(() => {});
      else await member.roles.remove(role).catch(() => {});
      return safeReply(interaction, { embeds: [successEmbed(`${role} ${commandName === 'addrole' ? 'added to' : 'removed from'} ${user}.`)] });
    }
    case 'verifyconfig': {
      if (!requireLevel(interaction, gconf, LEVEL.ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Administrator or higher.')], flags: MessageFlags.Ephemeral });
      const role = interaction.options.getRole('role');
      const channel = interaction.options.getChannel('channel');
      gconf.verification.enabled = true;
      gconf.verification.roleId = role.id;
      gconf.verification.channelId = channel.id;
      saveDB();
      return safeReply(interaction, { embeds: [successEmbed(`Verification configured. Run \`/verify\` to post the button in ${channel}.`)] });
    }
    case 'verify': {
      if (!requireLevel(interaction, gconf, LEVEL.ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Administrator or higher.')], flags: MessageFlags.Ephemeral });
      if (!gconf.verification.enabled) return safeReply(interaction, { embeds: [errorEmbed('Run `/verifyconfig` first.')], flags: MessageFlags.Ephemeral });
      const ch = await guild.channels.fetch(gconf.verification.channelId).catch(() => null);
      if (!ch) return safeReply(interaction, { embeds: [errorEmbed('Configured verification channel not found.')], flags: MessageFlags.Ephemeral });
      const embed = infoEmbed('Click the button below to verify yourself and gain access to the server.', '✅ Verification');
      const row = new ActionRowBuilder().addComponents(new ButtonBuilder().setCustomId('verify_button').setLabel('Verify').setStyle(ButtonStyle.Success));
      const msg = await ch.send({ embeds: [embed], components: [row] });
      gconf.verification.messageId = msg.id; saveDB();
      return safeReply(interaction, { embeds: [successEmbed('Verification panel posted.')], flags: MessageFlags.Ephemeral });
    }
    case 'serverstats': {
      if (!requireLevel(interaction, gconf, LEVEL.ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Administrator or higher.')], flags: MessageFlags.Ephemeral });
      const sub = interaction.options.getSubcommand();
      if (sub === 'setup') {
        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        const cats = { members: 'Members: 0', bots: 'Bots: 0', humans: 'Humans: 0', online: 'Online: 0', boosts: 'Boosts: 0' };
        for (const [key, name] of Object.entries(cats)) {
          const ch = await guild.channels.create({ name, type: ChannelType.GuildVoice, permissionOverwrites: [{ id: guild.roles.everyone, deny: [PermissionFlagsBits.Connect] }] }).catch(() => null);
          if (ch) gconf.serverstats.channels[key] = ch.id;
        }
        saveDB();
        await serverStatsTick();
        return safeReply(interaction, { embeds: [successEmbed('Server statistic channels created.')] });
      }
      for (const chId of Object.values(gconf.serverstats.channels)) {
        const ch = await guild.channels.fetch(chId).catch(() => null);
        if (ch) await ch.delete().catch(() => {});
      }
      gconf.serverstats.channels = {}; saveDB();
      return safeReply(interaction, { embeds: [successEmbed('Server statistic channels removed.')] });
    }

    case 'starboard': {
      if (!requireLevel(interaction, gconf, LEVEL.ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Administrator or higher.')], flags: MessageFlags.Ephemeral });
      const sub = interaction.options.getSubcommand();
      if (sub === 'setup') {
        gconf.starboard.enabled = true;
        gconf.starboard.channelId = interaction.options.getChannel('channel').id;
        gconf.starboard.threshold = interaction.options.getInteger('threshold');
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed('Starboard configured.')] });
      }
      gconf.starboard.enabled = false; saveDB();
      return safeReply(interaction, { embeds: [successEmbed('Starboard disabled.')] });
    }
    case 'reactionrole': {
      if (!requireLevel(interaction, gconf, LEVEL.ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Administrator or higher.')], flags: MessageFlags.Ephemeral });
      const sub = interaction.options.getSubcommand();
      if (sub === 'add') {
        const messageId = interaction.options.getString('messageid');
        const emoji = interaction.options.getString('emoji');
        const role = interaction.options.getRole('role');
        gconf.reactionroles.push({ messageId, emoji, roleId: role.id });
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed(`Reaction role added: ${emoji} → ${role}. Make sure to react ${emoji} on that message yourself so users can see it, or ask users to react.`)] });
      }
      if (sub === 'remove') {
        const messageId = interaction.options.getString('messageid');
        const emoji = interaction.options.getString('emoji');
        gconf.reactionroles = gconf.reactionroles.filter(r => !(r.messageId === messageId && r.emoji === emoji));
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed('Reaction role removed.')] });
      }
      const desc = gconf.reactionroles.length ? gconf.reactionroles.map(r => `${r.emoji} → <@&${r.roleId}> (msg: ${r.messageId})`).join('\n') : 'No reaction roles configured.';
      return safeReply(interaction, { embeds: [infoEmbed(desc, 'Reaction Roles')] });
    }
    case 'autopublish': {
      if (!requireLevel(interaction, gconf, LEVEL.ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Administrator or higher.')], flags: MessageFlags.Ephemeral });
      const sub = interaction.options.getSubcommand();
      const channel = interaction.options.getChannel('channel');
      if (sub === 'setup') {
        if (!gconf.autopublish.channels.includes(channel.id)) gconf.autopublish.channels.push(channel.id);
      } else {
        gconf.autopublish.channels = gconf.autopublish.channels.filter(id => id !== channel.id);
      }
      saveDB();
      return safeReply(interaction, { embeds: [successEmbed(`Auto-publish ${sub === 'setup' ? 'enabled' : 'disabled'} for ${channel}.`)] });
    }

    case 'application': {
      if (!requireLevel(interaction, gconf, LEVEL.ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Administrator or higher.')], flags: MessageFlags.Ephemeral });
      const sub = interaction.options.getSubcommand();
      if (sub === 'setup') {
        gconf.applications.reviewChannelId = interaction.options.getChannel('reviewchannel').id;
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed('Applications will now be reviewed in that channel.')] });
      }
      if (sub === 'panel') {
        if (!gconf.applications.reviewChannelId) return safeReply(interaction, { embeds: [errorEmbed('Run `/application setup` first.')], flags: MessageFlags.Ephemeral });
        const title = interaction.options.getString('title');
        const description = interaction.options.getString('description');
        const banner = interaction.options.getString('banner');
        const embed = new EmbedBuilder().setColor(COLORS.info).setTitle(title).setDescription(description).setTimestamp();
        if (banner) embed.setImage(banner);
        const row = new ActionRowBuilder().addComponents(new ButtonBuilder().setCustomId('application_apply').setLabel('📝 Apply').setStyle(ButtonStyle.Success));
        const msg = await interaction.channel.send({ embeds: [embed], components: [row] });
        const panelId = gconf.applications.nextPanelId++;
        gconf.applications.panels[panelId] = { channelId: interaction.channel.id, messageId: msg.id, title, description, banner: banner || null };
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed(`Application panel #${panelId} posted.`)], flags: MessageFlags.Ephemeral });
      }
      if (sub === 'addquestion') {
        if (gconf.applications.questions.length >= 5) return safeReply(interaction, { embeds: [errorEmbed('Maximum of 5 questions (Discord modal limit).')], flags: MessageFlags.Ephemeral });
        gconf.applications.questions.push(interaction.options.getString('question'));
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed('Question added.')] });
      }
      if (sub === 'removequestion') {
        const num = interaction.options.getInteger('number');
        if (num < 1 || num > gconf.applications.questions.length) return safeReply(interaction, { embeds: [errorEmbed('Invalid question number.')], flags: MessageFlags.Ephemeral });
        gconf.applications.questions.splice(num - 1, 1);
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed('Question removed.')] });
      }
      if (sub === 'listquestions') {
        const desc = gconf.applications.questions.length ? gconf.applications.questions.map((q, i) => `**${i + 1}.** ${q}`).join('\n') : 'No questions configured (a default set will be used).';
        return safeReply(interaction, { embeds: [infoEmbed(desc, 'Application Questions')] });
      }
      if (sub === 'list') {
        const status = interaction.options.getString('status');
        let subs = gconf.applications.submissions;
        if (status) subs = subs.filter(s => s.status === status);
        subs = subs.slice(-10).reverse();
        const desc = subs.length ? subs.map(s => `<@${s.userId}> — **${s.status}** — <t:${Math.floor(s.ts / 1000)}:R>`).join('\n') : 'No applications found.';
        return safeReply(interaction, { embeds: [infoEmbed(desc, 'Recent Applications')] });
      }
      break;
    }

    case 'aiconfig': {
      if (!requireLevel(interaction, gconf, LEVEL.SUPER_ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Super Admin or higher.')], flags: MessageFlags.Ephemeral });
      const sub = interaction.options.getSubcommand();
      if (sub === 'setprompt') {
        gconf.ai.customPrompt = interaction.options.getString('prompt').slice(0, 2000);
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed('AI system prompt updated.')] });
      }
      if (sub === 'viewprompt') {
        return safeReply(interaction, { embeds: [infoEmbed(gconf.ai.customPrompt || '*No custom prompt set — using default Jarvis persona.*', 'Current AI Prompt')] });
      }
      if (sub === 'resetprompt') {
        gconf.ai.customPrompt = '';
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed('AI system prompt reset to default.')] });
      }
      if (sub === 'clearmemory') {
        const user = interaction.options.getUser('user');
        if (user) delete gconf.ai.memory[user.id];
        else gconf.ai.memory = {};
        saveDB();
        return safeReply(interaction, { embeds: [successEmbed(user ? `Cleared AI memory for ${user}.` : 'Cleared all AI memory for this server.')] });
      }
      break;
    }

    case 'announcement': {
      if (!requireLevel(interaction, gconf, LEVEL.ADMIN)) return safeReply(interaction, { embeds: [errorEmbed('Requires Administrator or higher.')], flags: MessageFlags.Ephemeral });
      const channel = interaction.options.getChannel('channel');
      const title = interaction.options.getString('title');
      const messageText = interaction.options.getString('message');
      const ping = interaction.options.getString('ping') || 'none';
      const banner = interaction.options.getString('banner');
      const embed = new EmbedBuilder().setColor(COLORS.info).setTitle(`📢 ${title}`).setDescription(messageText).setTimestamp()
        .setFooter({ text: `Announcement by ${interaction.user.tag}` });
      if (banner) embed.setImage(banner);
      const content = ping === 'everyone' ? '@everyone' : ping === 'here' ? '@here' : undefined;
      const sent = await channel.send({ content, embeds: [embed] }).catch(() => null);
      if (!sent) return safeReply(interaction, { embeds: [errorEmbed('Could not send the announcement (check my permissions in that channel).')], flags: MessageFlags.Ephemeral });
      if (channel.type === ChannelType.GuildAnnouncement) sent.crosspost().catch(() => {});
      return safeReply(interaction, { embeds: [successEmbed(`Announcement posted in ${channel}.`)], flags: MessageFlags.Ephemeral });
    }

    default:
      return safeReply(interaction, { embeds: [errorEmbed('Unknown command.')], flags: MessageFlags.Ephemeral });
  }
}

// ---------------------------------------------------------------------------
// TICKET SYSTEM COMMAND HANDLER
// ---------------------------------------------------------------------------
async function handleTicketCommand(interaction, gconf) {
  const sub = interaction.options.getSubcommand();
  const guild = interaction.guild;
  const adminSubs = ['setup', 'panel', 'panels', 'editpanel', 'deletepanel', 'closeall', 'addtype', 'listtypes', 'edittype', 'deletetype', 'config'];

  if (adminSubs.includes(sub) && !requireLevel(interaction, gconf, LEVEL.ADMIN)) {
    return safeReply(interaction, { embeds: [errorEmbed('Requires Administrator or higher.')], flags: MessageFlags.Ephemeral });
  }

  if (sub === 'setup') {
    gconf.tickets.categoryId = interaction.options.getChannel('category').id;
    gconf.tickets.supportRoles = [interaction.options.getRole('supportrole').id];
    const logch = interaction.options.getChannel('logchannel');
    if (logch) gconf.tickets.logChannel = logch.id;
    saveDB();
    return safeReply(interaction, { embeds: [successEmbed('Ticket system configured.')] });
  }
  if (sub === 'panel') {
    if (!gconf.tickets.categoryId) return safeReply(interaction, { embeds: [errorEmbed('Run `/ticket setup` first.')], flags: MessageFlags.Ephemeral });
    const title = interaction.options.getString('title');
    const description = interaction.options.getString('description');
    const banner = interaction.options.getString('banner');
    const types = Object.keys(gconf.tickets.types);
    const embed = new EmbedBuilder().setColor(COLORS.info).setTitle(title).setDescription(description).setTimestamp();
    if (banner) embed.setImage(banner);
    let row;
    if (types.length) {
      row = new ActionRowBuilder().addComponents(
        new StringSelectMenuBuilder().setCustomId('ticket_create_select').setPlaceholder('Select a ticket type')
          .addOptions(types.slice(0, 25).map(t => ({ label: t, value: t, emoji: gconf.tickets.types[t].emoji || undefined }))),
      );
    } else {
      row = new ActionRowBuilder().addComponents(new ButtonBuilder().setCustomId('ticket_create_default').setLabel('🎫 Open Ticket').setStyle(ButtonStyle.Primary));
    }
    const msg = await interaction.channel.send({ embeds: [embed], components: [row] });
    const panelId = gconf.tickets.nextPanelId++;
    gconf.tickets.panels[panelId] = { channelId: interaction.channel.id, messageId: msg.id, title, description, banner: banner || null };
    saveDB();
    return safeReply(interaction, { embeds: [successEmbed(`Panel #${panelId} posted.`)], flags: MessageFlags.Ephemeral });
  }
  if (sub === 'panels') {
    const entries = Object.entries(gconf.tickets.panels);
    const desc = entries.length ? entries.map(([id, p]) => `**#${id}** — ${p.title} (<#${p.channelId}>)`).join('\n') : 'No panels created.';
    return safeReply(interaction, { embeds: [infoEmbed(desc, 'Ticket Panels')] });
  }
  if (sub === 'editpanel') {
    const id = interaction.options.getInteger('id');
    const panel = gconf.tickets.panels[id];
    if (!panel) return safeReply(interaction, { embeds: [errorEmbed('Panel not found.')], flags: MessageFlags.Ephemeral });
    const title = interaction.options.getString('title') || panel.title;
    const description = interaction.options.getString('description') || panel.description;
    const banner = interaction.options.getString('banner') || panel.banner;
    panel.title = title; panel.description = description; panel.banner = banner || null;
    const ch = await guild.channels.fetch(panel.channelId).catch(() => null);
    const msg = ch ? await ch.messages.fetch(panel.messageId).catch(() => null) : null;
    const editedEmbed = new EmbedBuilder().setColor(COLORS.info).setTitle(title).setDescription(description);
    if (banner) editedEmbed.setImage(banner);
    if (msg) msg.edit({ embeds: [editedEmbed] }).catch(() => {});
    saveDB();
    return safeReply(interaction, { embeds: [successEmbed(`Panel #${id} updated.`)] });
  }
  if (sub === 'deletepanel') {
    const id = interaction.options.getInteger('id');
    const panel = gconf.tickets.panels[id];
    if (!panel) return safeReply(interaction, { embeds: [errorEmbed('Panel not found.')], flags: MessageFlags.Ephemeral });
    const ch = await guild.channels.fetch(panel.channelId).catch(() => null);
    if (ch) { const msg = await ch.messages.fetch(panel.messageId).catch(() => null); if (msg) msg.delete().catch(() => {}); }
    delete gconf.tickets.panels[id]; saveDB();
    return safeReply(interaction, { embeds: [successEmbed(`Panel #${id} deleted.`)] });
  }
  if (sub === 'closeall') {
    await interaction.deferReply({ flags: MessageFlags.Ephemeral });
    const openRows = ticketDB.allOpenByGuild.all(guild.id);
    let count = 0;
    for (const row of openRows) {
      const ch = await guild.channels.fetch(row.channel_id).catch(() => null);
      if (ch) { await ch.delete().catch(() => {}); count++; }
      ticketDB.close.run(Date.now(), interaction.user.id, row.channel_id);
    }
    return safeReply(interaction, { embeds: [successEmbed(`Closed ${count} ticket(s).`)] });
  }
  if (sub === 'addtype') {
    const name = interaction.options.getString('name');
    const emoji = interaction.options.getString('emoji') || '🎫';
    gconf.tickets.types[name] = { label: name, emoji };
    saveDB();
    return safeReply(interaction, { embeds: [successEmbed(`Ticket type **${name}** added.`)] });
  }
  if (sub === 'listtypes') {
    const entries = Object.entries(gconf.tickets.types);
    return safeReply(interaction, { embeds: [infoEmbed(entries.length ? entries.map(([n, t]) => `${t.emoji} ${n}`).join('\n') : 'No ticket types configured.', 'Ticket Types')] });
  }
  if (sub === 'edittype') {
    const name = interaction.options.getString('name');
    if (!gconf.tickets.types[name]) return safeReply(interaction, { embeds: [errorEmbed('Type not found.')], flags: MessageFlags.Ephemeral });
    gconf.tickets.types[name].emoji = interaction.options.getString('emoji');
    saveDB();
    return safeReply(interaction, { embeds: [successEmbed(`Type **${name}** updated.`)] });
  }
  if (sub === 'deletetype') {
    const name = interaction.options.getString('name');
    delete gconf.tickets.types[name]; saveDB();
    return safeReply(interaction, { embeds: [successEmbed(`Type **${name}** deleted.`)] });
  }
  if (sub === 'config') {
    return safeReply(interaction, { embeds: [configSummaryEmbed(gconf, 'tickets', guild.id)] });
  }

  const ticketRow = ticketDB.getOpenByChannel.get(interaction.channel.id);

  if (sub === 'add' || sub === 'remove') {
    if (!ticketRow) return safeReply(interaction, { embeds: [errorEmbed('This is not a ticket channel.')], flags: MessageFlags.Ephemeral });
    const user = interaction.options.getUser('user');
    if (sub === 'add') await interaction.channel.permissionOverwrites.edit(user.id, { ViewChannel: true, SendMessages: true });
    else await interaction.channel.permissionOverwrites.delete(user.id);
    return safeReply(interaction, { embeds: [successEmbed(`${user} ${sub === 'add' ? 'added to' : 'removed from'} the ticket.`)] });
  }
  if (sub === 'claim') {
    if (!ticketRow) return safeReply(interaction, { embeds: [errorEmbed('This is not a ticket channel.')], flags: MessageFlags.Ephemeral });
    ticketDB.claim.run(interaction.user.id, interaction.channel.id);
    return safeReply(interaction, { embeds: [successEmbed(`Ticket claimed by ${interaction.user}.`)] });
  }
  if (sub === 'close') {
    if (!ticketRow) return safeReply(interaction, { embeds: [errorEmbed('This is not a ticket channel.')], flags: MessageFlags.Ephemeral });
    await safeReply(interaction, { embeds: [warnEmbed('Closing this ticket in 5 seconds...')] });
    const logCh = gconf.tickets.logChannel ? await guild.channels.fetch(gconf.tickets.logChannel).catch(() => null) : null;
    if (logCh) logCh.send({ embeds: [infoEmbed(`Ticket <#${interaction.channel.id}> closed by ${interaction.user}.`, '🎫 Ticket Closed')] }).catch(() => {});
    ticketDB.close.run(Date.now(), interaction.user.id, interaction.channel.id);
    setTimeout(() => interaction.channel.delete().catch(() => {}), 5000);
    return;
  }
  if (sub === 'transcript') {
    if (!ticketRow) return safeReply(interaction, { embeds: [errorEmbed('This is not a ticket channel.')], flags: MessageFlags.Ephemeral });
    await interaction.deferReply();
    const rows = ticketDB.getTranscript.all(interaction.channel.id);
    let lines;
    if (rows.length) {
      lines = rows.map(r => `[${new Date(r.created_at).toISOString()}] ${r.author_tag}: ${r.content}${r.attachments ? ' ' + r.attachments : ''}`);
    } else {
      const messages = await interaction.channel.messages.fetch({ limit: 100 });
      const sorted = [...messages.values()].reverse();
      lines = sorted.map(m => `[${m.createdAt.toISOString()}] ${m.author.tag}: ${m.content}${m.attachments.size ? ' ' + [...m.attachments.values()].map(a => a.url).join(' ') : ''}`);
    }
    const buffer = Buffer.from(lines.join('\n'), 'utf8');
    const attachment = new AttachmentBuilder(buffer, { name: `transcript-${interaction.channel.id}.txt` });
    return safeReply(interaction, { content: 'Transcript generated:', files: [attachment] });
  }
  if (sub === 'stats') {
    const open = (ticketDB.countOpenByGuild.get(guild.id) || { c: 0 }).c;
    const closed = (ticketDB.countClosedByGuild.get(guild.id) || { c: 0 }).c;
    return safeReply(interaction, { embeds: [infoEmbed(`Open tickets: **${open}**\nTotal closed: **${closed}**`, '🎫 Ticket Stats')] });
  }
}

// ---------------------------------------------------------------------------
// TICKET CREATION HELPER
// ---------------------------------------------------------------------------
async function createTicket(interaction, gconf, type = 'general') {
  const guild = interaction.guild;
  if (!gconf.tickets.categoryId) return safeReply(interaction, { embeds: [errorEmbed('Ticket system is not configured yet.')], flags: MessageFlags.Ephemeral });

  const openForUser = (ticketDB.countOpenByUser.get(guild.id, interaction.user.id) || { c: 0 }).c;
  if (openForUser >= 3) return safeReply(interaction, { embeds: [errorEmbed('You already have the maximum number of open tickets (3).')], flags: MessageFlags.Ephemeral });

  const overwrites = [
    { id: guild.roles.everyone, deny: [PermissionFlagsBits.ViewChannel] },
    { id: interaction.user.id, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory] },
    { id: guild.members.me.id, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ManageChannels] },
  ];
  for (const roleId of gconf.tickets.supportRoles) {
    overwrites.push({ id: roleId, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory] });
  }

  const channel = await guild.channels.create({
    name: `ticket-${interaction.user.username}`.slice(0, 90),
    type: ChannelType.GuildText,
    parent: gconf.tickets.categoryId,
    permissionOverwrites: overwrites,
  }).catch(() => null);

  if (!channel) return safeReply(interaction, { embeds: [errorEmbed('Failed to create ticket channel (check my permissions/category).')], flags: MessageFlags.Ephemeral });

  ticketDB.create.run(channel.id, guild.id, interaction.user.id, type, Date.now());

  const embed = new EmbedBuilder().setColor(COLORS.info).setTitle(`🎫 Ticket — ${type}`)
    .setDescription(`Welcome ${interaction.user}, support will be with you shortly.\nUse the buttons below to manage this ticket.`).setTimestamp();
  const row = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('ticket_claim').setLabel('Claim').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('ticket_close').setLabel('Close').setStyle(ButtonStyle.Danger),
  );
  await channel.send({ content: gconf.tickets.supportRoles.map(r => `<@&${r}>`).join(' '), embeds: [embed], components: [row] });

  const logCh = gconf.tickets.logChannel ? await guild.channels.fetch(gconf.tickets.logChannel).catch(() => null) : null;
  if (logCh) logCh.send({ embeds: [infoEmbed(`New ticket <#${channel.id}> opened by ${interaction.user} (${type}).`, '🎫 Ticket Opened')] }).catch(() => {});

  return safeReply(interaction, { embeds: [successEmbed(`Ticket created: ${channel}`)], flags: MessageFlags.Ephemeral });
}

// ---------------------------------------------------------------------------
// BUTTON HANDLER
// ---------------------------------------------------------------------------
async function handleButton(interaction) {
  const gconf = getGuild(interaction.guild.id);
  const id = interaction.customId;

  if (id === 'help_prev' || id === 'help_next') {
    const pages = buildHelpPages();
    const page = id === 'help_next' ? 1 : 0;
    return interaction.update({ embeds: [pages[page]], components: [helpButtons(page)] });
  }

  if (id === 'ticket_create_default') return createTicket(interaction, gconf, 'general');

  if (id === 'ticket_claim') {
    const ticketRow = ticketDB.getOpenByChannel.get(interaction.channel.id);
    if (!ticketRow) return safeReply(interaction, { embeds: [errorEmbed('This is not a ticket channel.')], flags: MessageFlags.Ephemeral });
    ticketDB.claim.run(interaction.user.id, interaction.channel.id);
    return safeReply(interaction, { embeds: [successEmbed(`Claimed by ${interaction.user}.`)] });
  }
  if (id === 'ticket_close') {
    const ticketRow = ticketDB.getOpenByChannel.get(interaction.channel.id);
    if (!ticketRow) return safeReply(interaction, { embeds: [errorEmbed('This is not a ticket channel.')], flags: MessageFlags.Ephemeral });
    await safeReply(interaction, { embeds: [warnEmbed('Closing this ticket in 5 seconds...')] });
    const logCh = gconf.tickets.logChannel ? await interaction.guild.channels.fetch(gconf.tickets.logChannel).catch(() => null) : null;
    if (logCh) logCh.send({ embeds: [infoEmbed(`Ticket <#${interaction.channel.id}> closed by ${interaction.user}.`, '🎫 Ticket Closed')] }).catch(() => {});
    ticketDB.close.run(Date.now(), interaction.user.id, interaction.channel.id);
    setTimeout(() => interaction.channel.delete().catch(() => {}), 5000);
    return;
  }

  if (id === 'verify_button') {
    if (!gconf.verification.enabled) return safeReply(interaction, { embeds: [errorEmbed('Verification is not configured.')], flags: MessageFlags.Ephemeral });
    const role = interaction.guild.roles.cache.get(gconf.verification.roleId);
    if (!role) return safeReply(interaction, { embeds: [errorEmbed('Configured role not found.')], flags: MessageFlags.Ephemeral });
    await interaction.member.roles.add(role).catch(() => {});
    return safeReply(interaction, { embeds: [successEmbed('You are now verified!')], flags: MessageFlags.Ephemeral });
  }

  if (id === 'giveaway_enter') {
    const g = gconf.giveaways[interaction.message.id];
    if (!g || g.ended) return safeReply(interaction, { embeds: [errorEmbed('This giveaway has ended.')], flags: MessageFlags.Ephemeral });
    if (g.entrants.includes(interaction.user.id)) {
      g.entrants = g.entrants.filter(id2 => id2 !== interaction.user.id);
      saveDB();
      return safeReply(interaction, { embeds: [infoEmbed('You left the giveaway.')], flags: MessageFlags.Ephemeral });
    }
    g.entrants.push(interaction.user.id);
    saveDB();
    return safeReply(interaction, { embeds: [successEmbed('You entered the giveaway! Click again to leave.')], flags: MessageFlags.Ephemeral });
  }

  if (id === 'application_apply') {
    if (gconf.applications.pendingByUser[interaction.user.id]) {
      return safeReply(interaction, { embeds: [errorEmbed('You already have a pending application. Please wait for a response.')], flags: MessageFlags.Ephemeral });
    }
    const questions = gconf.applications.questions.length ? gconf.applications.questions : ['Why do you want to join?', 'Relevant experience?'];
    const modal = new ModalBuilder().setCustomId('application_modal').setTitle('Application Form');
    questions.slice(0, 5).forEach((q, i) => {
      modal.addComponents(new ActionRowBuilder().addComponents(
        new TextInputBuilder().setCustomId(`app_q${i}`).setLabel(q.slice(0, 45)).setStyle(TextInputStyle.Paragraph).setRequired(true).setMaxLength(1000),
      ));
    });
    return interaction.showModal(modal);
  }

  if (id.startsWith('application_accept_') || id.startsWith('application_deny_')) {
    if (!requireLevel(interaction, gconf, LEVEL.MOD)) return safeReply(interaction, { embeds: [errorEmbed('Requires Moderator or higher.')], flags: MessageFlags.Ephemeral });
    const accepted = id.startsWith('application_accept_');
    const userId = id.replace(accepted ? 'application_accept_' : 'application_deny_', '');
    const record = gconf.applications.submissions.find(s => s.userId === userId && s.status === 'pending');
    if (record) { record.status = accepted ? 'accepted' : 'denied'; record.reviewedBy = interaction.user.id; }
    delete gconf.applications.pendingByUser[userId];
    saveDB();

    const resultEmbed = accepted
      ? successEmbed(`Your application in **${interaction.guild.name}** was accepted!`, '✅ Application Accepted')
      : errorEmbed(`Your application in **${interaction.guild.name}** was not accepted at this time.`, '❌ Application Denied');
    if (gconf.applications.resultDMs) {
      const user = await client.users.fetch(userId).catch(() => null);
      if (user) await tryDM(user, resultEmbed);
    }

    const original = interaction.message;
    const updatedEmbeds = original.embeds.map(e => EmbedBuilder.from(e));
    if (updatedEmbeds[0]) updatedEmbeds[0].addFields({ name: 'Decision', value: `${accepted ? '✅ Accepted' : '❌ Denied'} by ${interaction.user}` });
    const disabledRow = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('application_accept_done').setLabel('Accept').setStyle(ButtonStyle.Success).setDisabled(true),
      new ButtonBuilder().setCustomId('application_deny_done').setLabel('Deny').setStyle(ButtonStyle.Danger).setDisabled(true),
    );
    await interaction.update({ embeds: updatedEmbeds, components: [disabledRow] }).catch(() => {});
    return;
  }

  if (id.startsWith('botconfig_')) return;
}

// ---------------------------------------------------------------------------
// SELECT MENU HANDLER
// ---------------------------------------------------------------------------
async function handleSelect(interaction) {
  const gconf = getGuild(interaction.guild.id);
  if (interaction.customId === 'botconfig_select') {
    const section = interaction.values[0];
    return interaction.update({ embeds: [configSummaryEmbed(gconf, section, interaction.guild.id)], components: [botconfigMenu()] });
  }
  if (interaction.customId === 'ticket_create_select') {
    const type = interaction.values[0];
    return createTicket(interaction, gconf, type);
  }
}

// ---------------------------------------------------------------------------
// MODAL HANDLER
// ---------------------------------------------------------------------------
async function handleModal(interaction) {
  const gconf = getGuild(interaction.guild.id);

  if (interaction.customId === 'application_modal') {
    const questions = gconf.applications.questions.length ? gconf.applications.questions : ['Why do you want to join?', 'Relevant experience?'];
    const answers = questions.slice(0, 5).map((q, i) => ({ q, a: interaction.fields.getTextInputValue(`app_q${i}`) }));

    gconf.applications.pendingByUser[interaction.user.id] = true;
    gconf.applications.submissions.push({ userId: interaction.user.id, tag: interaction.user.tag, answers, status: 'pending', reviewedBy: null, ts: Date.now() });
    saveDB();

    const embed = new EmbedBuilder().setColor(COLORS.info).setTitle(`📝 New Application — ${interaction.user.tag}`)
      .setThumbnail(interaction.user.displayAvatarURL())
      .addFields(answers.map(a => ({ name: a.q.slice(0, 256), value: a.a.slice(0, 1024) || 'No answer' })))
      .setFooter({ text: `User ID: ${interaction.user.id}` })
      .setTimestamp();
    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId(`application_accept_${interaction.user.id}`).setLabel('Accept').setStyle(ButtonStyle.Success),
      new ButtonBuilder().setCustomId(`application_deny_${interaction.user.id}`).setLabel('Deny').setStyle(ButtonStyle.Danger),
    );

    const reviewCh = gconf.applications.reviewChannelId ? await interaction.guild.channels.fetch(gconf.applications.reviewChannelId).catch(() => null) : null;
    if (reviewCh) await reviewCh.send({ embeds: [embed], components: [row] }).catch(() => {});

    return safeReply(interaction, { embeds: [successEmbed('Your application has been submitted for review!')], flags: MessageFlags.Ephemeral });
  }

  return safeReply(interaction, { embeds: [infoEmbed('Received.')], flags: MessageFlags.Ephemeral });
}

// ---------------------------------------------------------------------------
// GLOBAL ERROR HANDLING
// ---------------------------------------------------------------------------
process.on('uncaughtException', (err) => { console.error('Uncaught Exception:', err); flushDBSync(); });
process.on('unhandledRejection', (err) => { console.error('Unhandled Rejection:', err); flushDBSync(); });

// ---------------------------------------------------------------------------
// KEEP-ALIVE WEB SERVER
// ---------------------------------------------------------------------------
const app = express();
app.get('/', (req, res) => res.send('Bot is alive.'));
app.get('/health', (req, res) => res.json({ status: 'ok', uptime: process.uptime(), guilds: client.guilds.cache.size }));
app.listen(process.env.PORT || 3000, () => console.log(`Web server listening on port ${process.env.PORT || 3000}`));

// ---------------------------------------------------------------------------
// LOGIN (with retry/backoff so a transient gateway timeout doesn't kill the
// process on the first try)
// ---------------------------------------------------------------------------
async function loginWithRetry(maxAttempts = 5) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    console.log(`Starting Discord login (attempt ${attempt}/${maxAttempts})...`);
    try {
      await client.login(process.env.DISCORD_TOKEN);
      console.log('Discord login completed.');
      return;
    } catch (err) {
      console.error(`Login attempt ${attempt} failed:`, err?.message || err);
      if (attempt === maxAttempts) {
        console.error('All login attempts failed. Check DISCORD_TOKEN, privileged intents, and https://discordstatus.com.');
        process.exit(1);
      }
      const delayMs = 5000 * attempt;
      console.log(`Retrying in ${delayMs / 1000}s...`);
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
}

if (!process.env.DISCORD_TOKEN) {
  console.error('DISCORD_TOKEN is not set. Add it in your environment variables before starting the bot.');
  process.exit(1);
}

loginWithRetry();
