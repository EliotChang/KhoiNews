# 世界日報新聞魚 -- 內容結構規範

本文件定義管線產出的所有內容欄位。分為兩部分：**硬性限制**是平台/基礎設施的限制，違反會造成功能故障；**可調整值**是創作旋鈕——實驗時可自由調整。

---

## 硬性限制

由平台、API 或資料庫強制執行。違反會導致貼文被拒、發布失敗或管線錯誤。

### 平台字元限制

| 欄位 | 最大字元 | 常數 | 執行方式 |
|------|---------|------|---------|
| `script_10s` | 600 | `DEFAULT_SCRIPT_MAX_CHARS` | `_clamp_text` |
| `video_title_short` | 40 | `VIDEO_TITLE_MAX_CHARS` | `_normalize_video_title` |
| `caption_instagram` | 2200 | `INSTAGRAM_LIMIT` | `_clamp_text` |
| `caption_tiktok` | 2200 | `TIKTOK_LIMIT` | `_clamp_text` |
| `caption_youtube` | 5000 | `YOUTUBE_LIMIT` | `_clamp_text` |
| `caption_x` | 280 | `X_LIMIT` | `_smart_truncate` |

### Hashtag 分配

- 附加至：`caption_instagram`, `caption_tiktok`, `caption_x`
- **不**附加至：`caption_youtube`

### 正規化管線

每個內容欄位在儲存前經過以下流程：

1. `strip_urls_from_text` -- 從腳本中移除原始 URL
2. `_normalize_short_form_text` -- 去除 URL、壓縮空白、將 `!!` 簡化為 `!`
3. `_remove_disallowed_phrases` -- 去除禁用詞彙
4. `_clamp_text(value, platform_limit)` -- 超過限制時硬截斷加 `…`
5. `_ensure_hashtags_in_caption(caption, hashtags, platform_limit)` -- 在限制內附加缺少的 hashtag
6. `_append_source_attribution(caption, source_name, platform_limit)` -- 附加 `\n\n來源：{name}`

---

## 可調整 / 實驗性

這些是創作和編輯旋鈕。更改不會破壞管線——只會改變內容的風格、語氣或嚴格度。

### 腳本目標（繁體中文）

| 設定 | 目前值 | 常數 / 環境變數 | 備註 |
|-----|-------|----------------|------|
| 目標字數 | 160 | `DEFAULT_SCRIPT_TARGET_WORDS` | 中文字元數（非英文單字數）|
| 最大字數 | target + 30 | `DEFAULT_SCRIPT_MAX_WORDS_BUFFER` | |
| 最少字數 | 120 | `DEFAULT_SCRIPT_MIN_WORDS` | |
| 目標時長 | 35秒 | `DEFAULT_SCRIPT_TARGET_SECONDS` | |
| 最少句子 | 3 | `DEFAULT_SCRIPT_MIN_SENTENCES` | |
| 最多句子 | 8 | `DEFAULT_SCRIPT_MAX_SENTENCES` | |
| 最少來源事實 | 3 | `DEFAULT_SCRIPT_MIN_FACTS` | |

### 腳本驗證

| 檢查 | 行為 | 可放寬？ |
|-----|------|---------|
| 含義線索 | 腳本必須包含至少一個 `IMPLICATION_CUES` 中的詞彙 | 可以 |
| 禁用詞彙 | 從所有字幕和腳本中去除 | 可以 |
| 句子終結符 | 接受 `.!?。！？` | -- |

### Hashtag 預設值

| 設定 | 目前值 | 備註 |
|-----|-------|------|
| 數量範圍 | 3--7 | 由 `_normalize_hashtags` 強制 |
| 預設填充 | `["#國際新聞", "#時事", "#世界日報"]` | LLM 回傳少於 3 個時使用 |
| 正規化 | 支援中文字元、`#` 前綴、去重、最多 7 個 | |

### 字幕對齊（CJK 適配）

| 設定 | 目前值 | 備註 |
|-----|-------|------|
| 每行詞數 | 4 | `VIDEO_CAPTION_WORDS_PER_LINE` |
| 每提示最多詞數 | 4 | `VIDEO_CAPTION_MAX_WORDS_PER_CUE` |
| 最小對齊覆蓋率 | 0.5 | `VIDEO_CAPTION_MIN_ALIGNMENT_COVERAGE` |
| 分詞引擎 | jieba | 中文分詞 |
| 文字連接 | 無空格 | CJK 字元直接連接 |

---

## 實驗日誌

| 日期 | 變更內容 | 原因 | 結果 |
|-----|---------|------|------|
| | | | |
