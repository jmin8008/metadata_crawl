# metadata_crawl

## 프로젝트 개요
GEO 및 SRA 메타데이터 전수 수집 파이프라인 구축 — NCBI의 모든 공공 생물정보학 실험 메타데이터를 누락 없이 수집, 파싱하여 통합 데이터베이스를 구축한다.

## 목표
- [x] SRA 메타데이터 수집 (SRP, SRS, SRX, SRR) — 완료. 1.2억 레코드, 66GB SQLite
- [ ] GEO 메타데이터 수집 (GSE, GSM, GPL) — ~31.7만 GSE 대상
- [x] GEO-SRA 간 ID 매핑 (SRA 측 완료: Exp→Study/Sample, Run→Exp)
- [x] 관계형 DB 스키마 설계 및 데이터 적재 — 12 테이블 SQLite
- [x] 오류 제어 및 재개(Resume) 로직 구현
- [x] QC Report 생성

## 아키텍처
FTP Bulk Dump + Async 파싱 파이프라인:
- `src/downloaders/` — FTP/HTTP 다운로더 (resume 지원)
- `src/parsers/` — lxml iterparse 기반 SRA XML, GEO MINiML/SOFT 스트리밍 파서
- `src/db/` — PostgreSQL 스키마 (12 테이블), dataclass 모델, Async batch writer (UPSERT)
- `src/linkers/` — GEO↔SRA ID cross-referencing + E-utilities elink 보완
- `src/qc/` — QC report 생성 (누락 필드, 파싱 에러)
- `src/pipeline.py` — 전체 오케스트레이션 (다운로드→파싱→적재→링킹→QC)

## 기술 스택
- Python 3.11+ / lxml (XML 스트리밍) / aiohttp, httpx (비동기 HTTP)
- psycopg (async PostgreSQL) / pydantic-settings (설정)
- PostgreSQL 15+ (JSONB for 가변 속성)
- Docker + docker-compose (app + postgres)

## 수집 현황
- **SRA**: 완료 (FTP Full Dump 15.1GB → 파싱 → SQLite 66GB)
  - sra_studies: 703,897 / sra_samples: 40,329,192 / sra_experiments: 38,851,043 / sra_runs: 41,237,472
  - Run 정량 데이터(reads, bases, size)는 FTP 덤프에 미포함 → SRA_Accessions.tab으로 보완 필요
- **GEO**: 미수집 (~31.7만 GSE, 개별 FTP 다운로드 필요)

## Resume / 체크포인트
- **SRA**: FTP resume 지원 (파일 단위). 이미 완료됨.
- **GEO**: `data/geo_checkpoint.txt`에 완료된 GSE 목록 저장. 100개마다 체크포인트.
  - 인터넷 끊기거나 프로세스 중단 → 재시작 시 체크포인트에서 이어서 진행
  - 다운로드한 MINiML tgz는 파싱 후 즉시 삭제 (디스크 절약)
  - 10회 연속 에러 시 60초 대기 후 자동 재시도
- 재시작 명령: `uv run python -m src.pipeline --geo-only --log-level INFO`

## 컨벤션
- src layout (hatch wheel: packages = ["src"])
- DB 가변 속성은 JSONB, 핵심 필드는 정규 컬럼
- UPSERT (ON CONFLICT) 패턴으로 멱등성 보장
- 테스트: SQLite in-memory mock (PostgreSQL 없이도 실행 가능)
- PostgreSQL 없으면 SQLite 자동 fallback (data/metadata_crawl.sqlite)
