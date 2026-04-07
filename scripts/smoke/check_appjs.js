// Quick syntax check for app.js
const fs = require('fs');
const path = require('path');
const src = fs.readFileSync(path.join(__dirname, '../../nav_dashboard/web/static/app.js'), 'utf8');
try {
  new Function(src);
  console.log('SYNTAX OK, bytes:', src.length);
} catch (e) {
  console.log('SYNTAX ERROR:', e.message);
}
