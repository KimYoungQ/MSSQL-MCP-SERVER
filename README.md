## MSSQL-MCP-SERVER
Claude AI가 MSSQL 데이터베이스의 데이터를 읽고 분석할 수 있도록 하는 MCP 서버입니다.

## 주요 기능

### 테이블 관리
- 테이블 목록 조회
- 테이블 스키마 정보 확인
- 테이블 데이터 조회 (Preview → Execute 패턴)
- 테이블 통계 분석

### Stored Procedure 관리
- SP 목록 조회
- SP 정의 및 파라미터 확인
- **SP 실행 (Preview → Execute 패턴)** ✨ NEW!

### 보안 기능
- Preview-Execute 2단계 승인 시스템
- 5분 제한 시간 (Query Hash)
- API Key 인증
- SQL Injection 방지

## 시스템 아키텍처

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
