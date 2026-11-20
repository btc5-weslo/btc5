#!/usr/bin/env bash
# Assemble the single-file dashboard (served locally at / and published as the static HF Space).
cd "$(dirname "$0")"; { cat head.html body.html; echo '<script>'; cat app.js; echo '</script></body></html>'; } > ../static/index.html && cp ../static/index.html ../space/index.html && echo built
