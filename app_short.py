"""短波段第二策略 Streamlit entry point.

Streamlit Cloud 限制：(repo, branch, main_file_path) 必須唯一才能建獨立 App。
這個檔案只是 exec app.py，讓 Cloud 認為是不同 entry。
共用 app.py 完整邏輯，靠 secrets 的 strategy_tag = "short" 切換顯示。

新 Streamlit App 部署時設 main file path = app_short.py 即可。
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

# 直接執行 app.py，所有 streamlit UI 會 render
# globals() 確保 app.py 裡的全域變數正確存在於本 module 範圍
_app_path = os.path.join(_HERE, "app.py")
with open(_app_path, encoding="utf-8") as _f:
    _code = _f.read()
exec(compile(_code, _app_path, "exec"), globals())
