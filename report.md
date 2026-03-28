# 프로젝트 완료 리포트

## 1. 요구사항 요약

GEO 및 SRA 데이터베이스의 모든 공공 생물정보학 실험 메타데이터를 전수(Exhaustive) 수집하여 PostgreSQL 통합 DB에 적재한다.

**수집 대상:**
- **GEO**: GSE (Series), GSM (Sample), GPL (Platform) — 제목, 요약, 샘플 특성, 프로토콜, 기여자, PubMed ID 등
- **SRA**: SRP (Study), SRS (Sample), SRX (Experiment), SRR (Run) — 시퀀싱 전략, 장비, 리드 수, 파일 크기, 다운로드 경로 등
- **Cross-ref**: BioSample, BioProject, PubMed ID 간 연결

**핵심 요구사항:**
- NCBI E-utilities API + FTP 벌크 덤프 활용
- GEO↔SRA↔BioProject↔BioSample↔PubMed ID 매핑 유지
- API Rate Limit 준수, Resume 가능
- 가변 속성 JSONB 저장, 정규화된 관계형 스키마

**자동 보완 항목:**
- `[자동 추론]` 실행 환경: Python 3.11+, PostgreSQL 15+
- `[자동 추론]` 성공 기준: 엔티티별 수집+파싱+적재 성공, 테스트 통과, QC Report 생성
- `[자동 추론]` 예외 처리: API 타임아웃, 잘못된 XML, 빈 레코드, 메모리 관리

## 2. 첨부 자료 분석

- `제목없음.md`: 작업 의뢰서 (5개 섹션 — 목적, 수집 항목, 기술 요구사항, 저장 규격, 산출물)
- NCBI 공식 문서 및 API 리서치 결과 반영 (E-utilities, SRA XML 스키마, GEO SOFT/MINiML, FTP 덤프 구조)

## 3. 구현 방법론

### 검토한 접근법들

| 접근법 | 요약 | 선택 여부 | 이유 |
|--------|------|-----------|------|
| **A: FTP Bulk + Async** | FTP 덤프 다운로드 → lxml iterparse 스트리밍 → Async DB Writer | **선택** | 전수 수집 최적, 가장 가벼움 (580MB), 최다 테스트 (57개) |
| B: API + pysradb/GEOparse | 검증된 라이브러리로 API 기반 수집 | 미선택 | API 속도 제한으로 전수 수집 시 시간 과다, 라이브러리 의존도 높음 |
| C: Hybrid + Prefect | FTP 초기 + API 증분, Prefect 오케스트레이션 | 미선택 | 오버엔지니어링 (1.01GB 이미지), 현 단계에서 Prefect 불필요 |

### 최종 선택: 접근법 A — FTP Bulk Dump + Async 파싱 파이프라인

의뢰서의 핵심 목표가 "전수 수집(Exhaustive)"이므로, FTP 메타데이터 덤프 기반이 가장 완전한 수집을 보장한다.
lxml iterparse로 수 GB XML을 일정 메모리로 스트리밍 파싱하며, psycopg async로 고속 batch 적재한다.

## 4. 구현 상세

### 프로젝트 구조

```
metadata_crawl/
├── src/
│   ├── config.py                    # Pydantic Settings (환경변수 기반 설정)
│   ├── pipeline.py                  # 메인 파이프라인 오케스트레이션
│   ├── downloaders/
│   │   ├── ftp_downloader.py        # FTP 다운로더 (resume 지원, 진행률 콜백)
│   │   └── http_downloader.py       # httpx 기반 E-utilities API 클라이언트
│   ├── parsers/
│   │   ├── sra_xml_parser.py        # lxml iterparse SRA XML 스트리밍 파서
│   │   ├── geo_miniml_parser.py     # GEO MINiML XML 파서
│   │   └── geo_soft_parser.py       # GEO SOFT 텍스트 파서
│   ├── db/
│   │   ├── schema.py                # 12 테이블 DDL + GIN 인덱스
│   │   ├── models.py                # Python dataclass 모델
│   │   └── writer.py                # Async batch writer (UPSERT/ON CONFLICT)
│   ├── linkers/
│   │   └── id_mapper.py             # GEO↔SRA ID cross-referencing
│   └── qc/
│       └── reporter.py              # QC report 생성
├── tests/                           # 57개 테스트
│   ├── conftest.py, test_parsers.py, test_downloaders.py
│   ├── test_db.py, test_linkers.py, test_pipeline.py
├── pyproject.toml                   # PEP 621 (hatchling)
├── requirements.txt                 # pip fallback
├── environment.yml                  # conda fallback
├── Dockerfile                       # python:3.11-slim 기반
├── docker-compose.yml               # app + PostgreSQL 16
└── .dockerignore
```

### 핵심 코드 설명

| 모듈 | 역할 | 설계 의도 |
|------|------|-----------|
| `config.py` | `MC_` 접두사 환경변수로 DB/NCBI/FTP 설정 로드 | Pydantic Settings로 타입 안전성 + `.env` 파일 지원 |
| `ftp_downloader.py` | NCBI FTP에서 SRA XML 덤프/GEO MINiML 다운로드 | REST 명령으로 resume 지원, 진행률 콜백, 재시도 로직 |
| `http_downloader.py` | E-utilities API (esearch/efetch/elink) 호출 | httpx async, rate limit 준수, API key 자동 적용 |
| `sra_xml_parser.py` | SRA 전체 덤프 XML 스트리밍 파싱 | `EXPERIMENT_PACKAGE` 단위 iterparse → 메모리 일정 (수 GB 처리 가능) |
| `geo_miniml_parser.py` | GEO Series/Sample/Platform MINiML XML 파싱 | lxml 기반, 구조화된 dict 반환 |
| `geo_soft_parser.py` | GEO SOFT 텍스트 형식 파싱 | `^`/`!`/`#` 마커 기반 라인 파싱 |
| `schema.py` | 12 테이블 DDL (GEO 3 + SRA 4 + Cross-ref 3 + Pipeline 2) | JSONB로 가변 속성, GIN 인덱스로 검색 성능 확보 |
| `writer.py` | Async batch DB writer | `ON CONFLICT DO UPDATE` UPSERT로 멱등성 보장, batch insert |
| `id_mapper.py` | GEO↔SRA cross-reference 추출 + elink 보완 | 파싱 데이터 내 참조 필드 자동 추출 + API 보완 |
| `reporter.py` | QC report — 누락 필드, 파싱 에러, 테이블별 통계 | JSON 출력, 콘솔 요약 |
| `pipeline.py` | 전체 오케스트레이션 (다운로드→파싱→적재→링킹→QC) | CLI 인자 지원 (--geo-only, --sra-only, --gse) |

### DB 스키마 (ERD)

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ geo_series   │     │ geo_samples  │     │ geo_platforms│
│ (GSE)        │←───→│ (GSM)        │────→│ (GPL)        │
│ accession PK │     │ accession PK │     │ accession PK │
│ title        │     │ characteristics│   │ manufacturer │
│ summary      │     │ (JSONB)      │     │ organism     │
│ pubmed_ids   │     │ platform_ref │     └──────────────┘
│ (JSONB)      │     │ series_refs  │
│ contributors │     │ (JSONB)      │
│ (JSONB)      │     └──────────────┘
└──────────────┘
        │
        │ id_mappings (source↔target)
        ↓
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ sra_studies  │←────│sra_experiments│←───│ sra_runs     │
│ (SRP)        │     │ (SRX)        │     │ (SRR)        │
│ accession PK │     │ study_ref FK │     │ experiment_ref│
│ abstract     │     │ sample_ref FK│     │ total_spots  │
│ external_ids │     │ strategy     │     │ total_bases  │
│ (JSONB)      │     │ layout       │     │ sra_files    │
└──────────────┘     │ instrument   │     │ (JSONB)      │
                     └──────────────┘     └──────────────┘
        │                    │
        ↓                    ↓
┌──────────────┐     ┌──────────────┐
│ bioprojects  │     │ sra_samples  │
│ accession PK │     │ (SRS)        │
│ sra_study_ref│     │ accession PK │
│ geo_series_ref│    │ attributes   │
└──────────────┘     │ (JSONB)      │
                     └──────┬───────┘
                            │
                     ┌──────────────┐
                     │ biosamples   │
                     │ accession PK │
                     │ sra_sample_ref│
                     │ attributes   │
                     │ (JSONB)      │
                     └──────────────┘

+ pipeline_checkpoints (resume 상태 저장)
+ qc_reports (QC 감사 로그)
+ id_mappings (N:M cross-reference 허브)
```

### 의존성

| 패키지 | 용도 |
|--------|------|
| lxml | XML 스트리밍 파싱 (iterparse) |
| aiohttp | 비동기 HTTP 클라이언트 |
| httpx | E-utilities API 호출 |
| psycopg + psycopg-pool | Async PostgreSQL 드라이버 |
| pydantic-settings | 환경변수 기반 설정 관리 |
| tenacity | 재시도 로직 (exponential backoff) |
| tqdm | 다운로드/파싱 진행률 표시 |
| aiofiles | 비동기 파일 I/O |

## 5. 테스트 결과

```
tests/test_downloaders.py    14 passed
tests/test_linkers.py         6 passed
tests/test_parsers.py        17 passed
tests/test_pipeline.py        8 passed
tests/test_db.py             12 passed
─────────────────────────────────────
총 57/57 passed (0.83s)
```

## 6. 디버깅 이력

| 문제 | 증상 | 해결 |
|------|------|------|
| hatchling 패키지 탐색 실패 | "Unable to determine which files to ship" | `packages = ["src"]` 명시 |
| SRA SAMPLE_ATTRIBUTE 파싱 | 속성이 빈 dict | XML child element 우선 탐색으로 변경 |

전략 전환 없이 직접 수정으로 모두 해결됨.

## 7. 사용 가이드

### 설치 및 실행 (uv — 권장)
```bash
cd metadata_crawl
uv sync --all-extras

# 테스트
uv run python -m pytest tests/ -v

# 전체 파이프라인
uv run python -m src.pipeline

# GEO만
uv run python -m src.pipeline --geo-only

# SRA만
uv run python -m src.pipeline --sra-only

# 특정 GSE
uv run python -m src.pipeline --gse GSE12345
```

### 설치 및 실행 (Docker)
```bash
# PostgreSQL + 파이프라인 기동
docker compose up

# 파이프라인만 실행 (DB는 이미 기동 중)
docker compose run pipeline --sra-only
```

### 설치 및 실행 (conda — fallback)
```bash
conda env create -f environment.yml
conda activate metadata-crawl
pip install -e .
python -m src.pipeline
```

### 설정

환경변수 (`MC_` 접두사) 또는 `.env` 파일:

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `MC_DB_HOST` | localhost | PostgreSQL 호스트 |
| `MC_DB_PORT` | 5432 | PostgreSQL 포트 |
| `MC_DB_NAME` | metadata_crawl | DB 이름 |
| `MC_DB_USER` | postgres | DB 사용자 |
| `MC_DB_PASSWORD` | postgres | DB 비밀번호 |
| `MC_NCBI_API_KEY` | (없음) | NCBI API 키 (있으면 10 req/s) |
| `MC_NCBI_EMAIL` | user@example.com | NCBI 등록 이메일 |
| `MC_BATCH_SIZE` | 500 | DB batch insert 크기 |
| `MC_RESUME_ENABLED` | true | Resume 기능 활성화 |
| `MC_DOWNLOAD_DIR` | ./data/downloads | 다운로드 디렉토리 |

## 8. 알려진 제한사항 및 향후 개선사항

**제한사항:**
- 실제 NCBI FTP/API 연결은 네트워크 환경 및 API key 필요
- SRA Full XML 덤프는 수백 GB이므로 실 운영 시 디스크/메모리 모니터링 필요
- GEO 전체 GSE 목록 자동 탐색은 FTP 디렉토리 재귀 탐색 추가 구현 필요 (현재는 `--gse` 개별 지정)

**향후 개선:**
- GEO FTP 디렉토리 자동 크롤링 (GSE 전체 목록 수집)
- 증분 업데이트 파이프라인 (신규/수정 레코드만 수집)
- Prefect/Airflow 등 워크플로우 오케스트레이션 통합
- Grafana 모니터링 대시보드
- AWS S3/GCP GCS 다운로드 경로 지원
