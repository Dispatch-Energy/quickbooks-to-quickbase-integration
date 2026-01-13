# Twilio SMS Integration Setup

This guide explains how to set up Twilio to automatically receive and forward Intuit verification codes to the QB Sync app.

## How It Works

1. **Update Intuit account phone number** to your Twilio number
2. When sync triggers SMS verification, **Intuit sends code to Twilio number**
3. **Twilio webhook** calls your app's `/twilio/sms` endpoint
4. App **extracts the 6-digit code** and uses it automatically
5. No manual intervention needed!

## Twilio Setup Steps

### 1. Get a Twilio Phone Number

1. Sign up at [twilio.com](https://www.twilio.com)
2. Go to **Phone Numbers** → **Manage** → **Buy a number**
3. Buy a number with **SMS capability** (any US number works)
4. Note the phone number (e.g., `+1 555-123-4567`)

### 2. Configure the Webhook

1. Go to **Phone Numbers** → **Manage** → **Active Numbers**
2. Click on your phone number
3. Scroll to **Messaging Configuration**
4. Under **"A message comes in"**, set:
   - **Webhook URL**: `https://qb-sync.kindfield-16cb452b.eastus.azurecontainerapps.io/twilio/sms`
   - **HTTP Method**: `POST`
5. Click **Save configuration**

### 3. Update Intuit Account

1. Log in to [accounts.intuit.com](https://accounts.intuit.com)
2. Go to **Sign in & security** → **Phone number**
3. Update the phone number to your **Twilio number**
4. Verify the number (Twilio will receive the verification code and forward it)

## Testing

1. Trigger a sync:
   ```bash
   curl -X POST https://qb-sync.kindfield-16cb452b.eastus.azurecontainerapps.io/sync
   ```

2. Watch the logs - you should see:
   ```
   Twilio SMS received from +1XXXXXXXXXX: Your Intuit verification...
   Extracted verification code: 12****
   Got code: 12****
   Verification successful!
   ```

3. The sync should complete automatically without manual code entry!

## Webhook Details

**Endpoint**: `POST /twilio/sms`

**Twilio sends** (form-encoded):
- `From`: Sender phone number (Intuit's number)
- `To`: Your Twilio number
- `Body`: SMS message text
- `MessageSid`: Twilio message ID

**App extracts**: Any 6-digit number from the message body

**Response**: Empty TwiML (no reply SMS sent)

## Message Format

Intuit verification messages look like:
```
Your Intuit verification code is 123456. It expires in 10 minutes. Don't share it with anyone.
```

The app uses regex `\b(\d{6})\b` to extract the code.

## Troubleshooting

### Webhook not receiving messages
- Check Twilio console **Monitor** → **Logs** → **Messaging**
- Verify webhook URL is correct and accessible
- Ensure your app is running and `/twilio/sms` returns 200

### Code not being extracted
- Check app logs for the SMS body
- Verify the message contains a 6-digit code
- The regex looks for any standalone 6-digit number

### Verification still timing out
- Intuit may send to a different number than expected
- Check if Intuit has multiple phone numbers on file
- Verify the Twilio number is set as primary in Intuit

## Security Notes

- The `/twilio/sms` endpoint is public (Twilio needs to call it)
- Consider adding Twilio request validation for production:
  ```python
  from twilio.request_validator import RequestValidator
  ```
- The verification code is only valid for ~10 minutes
- Codes are cleared after use

## Costs

- Twilio phone number: ~$1.15/month
- Incoming SMS: ~$0.0079 per message
- Estimated monthly cost: ~$2-3 for daily syncs