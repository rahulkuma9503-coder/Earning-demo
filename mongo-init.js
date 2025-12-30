db.createUser({
  user: "botadmin",
  pwd: "botpassword",
  roles: [
    { role: "readWrite", db: "telegram_bot" },
    { role: "dbAdmin", db: "telegram_bot" }
  ]
});

db.createCollection("users");
db.createCollection("channels");
db.createCollection("referrals");
db.createCollection("transactions");
db.createCollection("withdrawals");

// Create indexes
db.users.createIndex({ user_id: 1 }, { unique: true });
db.users.createIndex({ referral_code: 1 }, { unique: true });
db.channels.createIndex({ chat_id: 1 }, { unique: true });
db.referrals.createIndex({ referrer_id: 1, referred_id: 1 });
