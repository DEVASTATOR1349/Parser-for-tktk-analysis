/**
 * Парсер подписчиков — Google Apps Script Web App
 * Принимает POST через doPost(), раскладывает по листам.
 *
 * Поддерживаемые типы запросов:
 *   "batch_write"   — запись массива строк в лист «Статистика SQL»
 *   "batch_errors"  — запись массива ошибок в лист «Ошибки»
 */

function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents);
    const type = data.type;

    switch (type) {
      case "batch_write":
        return handleBatchWrite(data);
      case "batch_errors":
        return handleBatchErrors(data);
      default:
        return ContentService.createTextOutput("UNKNOWN_TYPE: " + type);
    }
  } catch (err) {
    return ContentService.createTextOutput("ERROR: " + err.message);
  }
}


// ──────────────────────────────────────────────────
//  Батч-запись в лист «Статистика SQL»
// ──────────────────────────────────────────────────
function handleBatchWrite(data) {
  // data.tab = "Статистика новая" (опционально)
  // data.rows = [[date, client, platform, followers], ...]
  const rows = data.rows;

  if (!rows || rows.length === 0) {
    return ContentService.createTextOutput("OK");
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const tabName = data.tab || "ДанныеПарсинга";

  let sheet = ss.getSheetByName(tabName);
  if (!sheet) {
    sheet = ss.insertSheet(tabName);
    sheet.getRange(1, 1, 1, 4).setValues([["Дата", "Клиент", "Площадка", "Подписчиков"]]);
  }

  const lastRow = sheet.getLastRow();
  sheet.getRange(lastRow + 1, 1, rows.length, 4).setValues(rows);

  return ContentService.createTextOutput("OK");
}


// ──────────────────────────────────────────────────
//  Батч-запись ошибок в лист «Ошибки»
// ──────────────────────────────────────────────────
function handleBatchErrors(data) {
  // data.rows = [[date, client, link, error], ...]
  const rows = data.rows;

  if (!rows || rows.length === 0) {
    return ContentService.createTextOutput("OK");
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const tabName = "Ошибки";

  let sheet = ss.getSheetByName(tabName);
  if (!sheet) {
    sheet = ss.insertSheet(tabName);
    sheet.getRange(1, 1, 1, 4).setValues([["Дата", "Клиент", "Ссылка", "Ошибка"]]);
  }

  const lastRow = sheet.getLastRow();
  sheet.getRange(lastRow + 1, 1, rows.length, 4).setValues(rows);

  return ContentService.createTextOutput("OK");
}
