## MSSQL-MCP-SERVER
Claude AI가 MSSQL 데이터베이스의 데이터를 읽고 분석할 수 있도록 하는 MCP 서버이다.

시스템 아키텍처

```
Claude AI (클라이언트)
    ↓ 
MCP 서버 (Python - FastMCP)
    ↓ 
API Gateway (Node.js + Express)
    ↓
MSSQL Database
```


의존성

```txt
# requirements.txt
mcp
httpx
python-dotenv
```

설치 :
```bash
pip install -r requirements.txt
```

실행 : 
```
초기 :
uv init
uv venv
.venv\Scripts\activate
uv add mcp[cli] httpx
new-item mssql_server.py

이후 : 
.venv\Scripts\activate
```

환경 변수

```bash
# .env
API_GATEWAY_URL=endpoint-here
API_KEY=api-key-here
```
