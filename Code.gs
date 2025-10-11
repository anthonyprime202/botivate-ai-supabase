/**
 * CONFIG
 */
const WEBHOOK_URL = "backend-url/webhook/sync"; 
const WEBHOOK_SECRET = "webhook-secret-passphrase";
const ALLOWED_SHEETS = [
  "Checklist",
  "Delegation",
  "Purchase Intransit",
  "Purchase Receipt",
  "Orders Pending",
  "Sales Invoices",
  "Collection Pending",
  "Production Orders",
  "Job Card Production",
  "PO Pending"
];

/**
 * 1) Publish only allowed sheets as JSON
 */
function doGet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const out = {};

  ss.getSheets().forEach(sheet => {
    const name = sheet.getName();
    if (ALLOWED_SHEETS.indexOf(name) === -1) return; // skip

    const data = sheet.getDataRange().getValues();
    if (!data || data.length < 2) return;

    const headers = data.shift();
    const rows = data.map(row => {
      const obj = {};
      headers.forEach((h, i) => (obj[h] = row[i]));
      return obj;
    });

    out[name] = rows;
  });

  return ContentService.createTextOutput(JSON.stringify(out))
    .setMimeType(ContentService.MimeType.JSON);
}


/**
 * 2) Function to call the FastAPI webhook.
 */
function callWebhook() {
  try {
    const options = {
      'method': 'post',
      'contentType': 'application/json',
      'headers': {
        'X-Webhook-Secret': WEBHOOK_SECRET
      },
      'muteHttpExceptions': true // Prevents script from stopping on HTTP errors
    };
    
    const response = UrlFetchApp.fetch(WEBHOOK_URL, options);
    Logger.log('Webhook Response:', response.getContentText());
  } catch (e) {
    Logger.log('Error calling webhook:', e.toString());
  }
}

/**
 * 3) The trigger function that runs on every edit.
 * This needs to be set up in the Triggers section.
 */
function onSheetEdit(e) {
  // The 'e' object contains info about the edit, but we don't need it.
  // We just need to know that an edit occurred.
  Logger.log("Sheet edited, calling webhook...");
  callWebhook();
}