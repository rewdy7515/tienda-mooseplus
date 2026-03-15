const webpush = require("web-push");

const keys = webpush.generateVAPIDKeys();

process.stdout.write(
  `${JSON.stringify(
    {
      WEB_PUSH_VAPID_PUBLIC_KEY: keys.publicKey,
      WEB_PUSH_VAPID_PRIVATE_KEY: keys.privateKey,
      WEB_PUSH_SUBJECT: "mailto:soporte@mooseplus.com",
    },
    null,
    2,
  )}\n`,
);
