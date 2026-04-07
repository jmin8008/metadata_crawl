# SRA 메타데이터 전수 수집 보고서

**수집일**: 2026-03-26
**데이터 소스**: NCBI SRA FTP Full Dump (2026-03-16 스냅샷)
**원본 파일**: `NCBI_SRA_Metadata_Full_20260316.tar.gz` (15.1GB)
**적재 DB**: SQLite (`metadata_crawl.sqlite`, 66GB)
**에러**: 0건

---

## 1. 수집 규모 요약

| 테이블 | 레코드 수 | 설명 |
|--------|-----------|------|
| **sra_studies** | 703,897 | 연구/프로젝트 단위 (SRP) |
| **sra_samples** | 40,329,192 | 생물학적 샘플 (SRS) |
| **sra_experiments** | 38,851,043 | 시퀀싱 실험 설정 (SRX) |
| **sra_runs** | 41,237,472 | 실제 시퀀싱 실행 (SRR) |
| **총 레코드** | **121,121,604** | 약 1억 2천만 건 |

---

## 2. 데이터 소스 구조 (FTP 덤프)

NCBI FTP 서버의 SRA 메타데이터 전체 덤프는 submission 단위로 디렉토리가 구성되어 있다.

| 파일 패턴 | XML 루트 태그 | 파싱 대상 | 추출 항목 |
|-----------|--------------|-----------|-----------|
| `{ID}.study.xml` | `<STUDY_SET>` → `<STUDY>` | 연구 정보 | accession, title, abstract, study_type, external_ids (BioProject, GEO, PubMed) |
| `{ID}.sample.xml` | `<SAMPLE_SET>` → `<SAMPLE>` | 샘플 정보 | accession, taxon_id, scientific_name, attributes (KEY-VALUE 쌍) |
| `{ID}.experiment.xml` | `<EXPERIMENT_SET>` → `<EXPERIMENT>` | 실험 설정 | accession, strategy, source, selection, layout, instrument_model, study_ref, sample_ref |
| `{ID}.run.xml` | `<RUN_SET>` → `<RUN>` | 실행 데이터 | accession, total_spots, total_bases, size, experiment_ref, sra_files |
| `{ID}.submission.xml` | `<SUBMISSION>` | (스킵) | 제출 메타정보 — 분석 불필요 |

### 파싱 방식

- **lxml iterparse** 스트리밍: 메모리 일정 (수십 GB XML도 처리 가능)
- tar.gz를 압축 해제 없이 스트리밍 읽기 → 각 XML 파일을 엔티티별 파서로 처리
- 파서는 XML attribute (`accession="SRP..."`) + 자식 태그 텍스트 (`<TITLE>...</TITLE>`) + TAG/VALUE 쌍을 dict로 변환

---

## 3. 테이블별 스키마 및 필드 완결성

### 3-1. sra_studies (703,897건)

| 필드 | 설명 | NULL 비율 |
|------|------|-----------|
| accession | SRP 번호 (PK) | 0% |
| alias | GEO GSE 번호 등 별칭 | 0.01% |
| center_name | 등록 기관 (GEO, DDBJ 등) | 0.14% |
| title | 연구 제목 | 0% |
| abstract | 연구 초록 | **6.88%** |
| study_type | 연구 유형 (Other, WGS 등) | 0% |
| external_ids | BioProject/GEO/PubMed 링크 (JSON) | **1.54%** |

### 3-2. sra_samples (40,329,192건)

| 필드 | 설명 | NULL 비율 |
|------|------|-----------|
| accession | SRS 번호 (PK) | 0% |
| alias | BioSample 번호 등 별칭 | 0% |
| title | 샘플 제목 | **26.82%** |
| taxon_id | NCBI Taxonomy ID | 0% |
| scientific_name | 학명 | 0.10% |
| attributes | 샘플 속성 (JSON, KEY-VALUE) | 0.21% |
| external_ids | BioSample 등 외부 ID (JSON) | 0% |

### 3-3. sra_experiments (38,851,043건)

| 필드 | 설명 | NULL 비율 |
|------|------|-----------|
| accession | SRX 번호 (PK) | 0% |
| title | 실험 제목 | **8.98%** |
| study_ref | 연결된 SRP 번호 (FK) | 0% |
| sample_ref | 연결된 SRS 번호 (FK) | 0% |
| strategy | 시퀀싱 전략 (RNA-Seq, WGS 등) | 0% |
| source | 라이브러리 소스 (GENOMIC 등) | 0% |
| selection | 선택 방법 (RANDOM 등) | 0% |
| layout | PAIRED / SINGLE | 0% |
| instrument_model | 장비 모델 (NovaSeq 6000 등) | 0% |

### 3-4. sra_runs (41,237,472건)

| 필드 | 설명 | NULL 비율 |
|------|------|-----------|
| accession | SRR 번호 (PK) | 0% |
| experiment_ref | 연결된 SRX 번호 (FK) | 0% |
| total_spots | 총 리드 수 | **100%** |
| total_bases | 총 염기쌍 수 | **100%** |
| size | 파일 크기 | **100%** |
| avg_length | 평균 리드 길이 | **100%** |
| sra_files | 다운로드 URL 목록 (JSON) | **100%** |

> **주의**: run.xml의 `total_spots`, `total_bases`, `size` 등은 FTP 덤프의 run.xml에 포함되지 않는 경우가 많다.
> 이 값들은 E-utilities API의 `efetch` 또는 `SRA_Accessions.tab` 파일에서 보완 수집이 필요하다.

---

## 4. 엔티티 간 관계 (ID 매핑)

SRA 데이터는 4계층 구조로 연결된다:

```
STUDY (SRP) ──< EXPERIMENT (SRX) ──< RUN (SRR)
                    │
                SAMPLE (SRS)
```

| 관계 | 연결 방식 | 설명 |
|------|-----------|------|
| Study → Experiment | experiment.study_ref | 1:N (하나의 연구에 여러 실험) |
| Experiment → Sample | experiment.sample_ref | N:1 (여러 실험이 같은 샘플 사용 가능) |
| Experiment → Run | run.experiment_ref | 1:N (하나의 실험에 여러 런) |
| Study → BioProject | study.external_ids["BioProject"] | 1:1 (PRJNA 번호) |
| Study → GEO | study.external_ids["GEO"] 또는 alias | 1:1 (GSE 번호, GEO 등록 시) |
| Sample → BioSample | sample.external_ids["BioSample"] | 1:1 (SAMN 번호) |
| Study → PubMed | study.external_ids["pubmed"] | 1:N (관련 논문) |

---

## 5. 수집 데이터 분포 분석

### 5-1. 시퀀싱 전략 (Sequencing Strategy)

| 순위 | 전략 | 실험 수 | 비율 |
|------|------|---------|------|
| 1 | AMPLICON | 17,049,392 | 43.9% |
| 2 | WGS (전장 유전체) | 8,345,926 | 21.5% |
| 3 | RNA-Seq (전사체) | 6,522,461 | 16.8% |
| 4 | OTHER | 2,664,505 | 6.9% |
| 5 | RAD-Seq | 698,350 | 1.8% |
| 6 | Targeted-Capture | 580,356 | 1.5% |
| 7 | WXS (엑솜) | 540,842 | 1.4% |
| 8 | WGA | 428,432 | 1.1% |
| 9 | ChIP-Seq (후성유전) | 387,122 | 1.0% |
| 10 | CLONE | 288,469 | 0.7% |
| 11 | Bisulfite-Seq (메틸화) | 284,068 | 0.7% |
| 12 | ATAC-seq (크로마틴 접근성) | 204,634 | 0.5% |
| 13 | miRNA-Seq | 175,803 | 0.5% |
| 14 | GBS | 156,306 | 0.4% |
| 15 | Hi-C (3D 유전체) | 70,453 | 0.2% |

> AMPLICON(PCR 기반)이 전체의 44%로 압도적 1위 — COVID-19 대유행 기간 SARS-CoV-2 감시 시퀀싱의 영향.

### 5-2. 시퀀싱 장비 (Instrument Model)

| 순위 | 장비 | 실험 수 | 비율 |
|------|------|---------|------|
| 1 | Illumina NovaSeq 6000 | 10,562,423 | 27.2% |
| 2 | Illumina MiSeq | 9,291,424 | 23.9% |
| 3 | Illumina HiSeq 2500 | 3,896,718 | 10.0% |
| 4 | Illumina HiSeq 2000 | 2,523,256 | 6.5% |
| 5 | NextSeq 500 | 1,976,024 | 5.1% |
| 6 | Illumina HiSeq 4000 | 1,583,015 | 4.1% |
| 7 | NextSeq 550 | 1,316,416 | 3.4% |
| 8 | HiSeq X Ten | 1,086,790 | 2.8% |
| 9 | NextSeq 2000 | 722,732 | 1.9% |
| 10 | Sequel II (PacBio) | 694,026 | 1.8% |
| 11 | GridION (Oxford Nanopore) | 482,097 | 1.2% |
| 12 | MinION (Oxford Nanopore) | 370,300 | 1.0% |
| 13 | AB 310 Genetic Analyzer | 328,526 | 0.8% |
| 14 | Illumina HiSeq 3000 | 323,181 | 0.8% |

> Illumina가 전체의 ~85% 점유. NovaSeq 6000이 1위, MiSeq가 2위.
> Long-read (PacBio Sequel II + ONT GridION/MinION) 합계 ~4%.

### 5-3. 라이브러리 레이아웃

| 레이아웃 | 실험 수 | 비율 |
|----------|---------|------|
| PAIRED (페어드엔드) | 30,089,991 | 77.5% |
| SINGLE (싱글엔드) | 8,761,052 | 22.5% |

### 5-4. 생물종 (Organism) TOP 15

| 순위 | 생물종 | 샘플 수 | 비율 |
|------|--------|---------|------|
| 1 | SARS-CoV-2 | 9,227,826 | 22.9% |
| 2 | Homo sapiens (사람) | 5,857,818 | 14.5% |
| 3 | Mus musculus (마우스) | 2,868,401 | 7.1% |
| 4 | soil metagenome (토양) | 1,184,388 | 2.9% |
| 5 | human gut metagenome | 1,122,846 | 2.8% |
| 6 | metagenome (기타) | 1,026,284 | 2.5% |
| 7 | gut metagenome | 579,199 | 1.4% |
| 8 | Escherichia coli (대장균) | 566,796 | 1.4% |
| 9 | Salmonella enterica | 518,644 | 1.3% |
| 10 | human metagenome | 377,061 | 0.9% |
| 11 | mouse gut metagenome | 342,506 | 0.8% |
| 12 | marine metagenome (해양) | 333,825 | 0.8% |
| 13 | Plasmodium falciparum (말라리아) | 331,623 | 0.8% |
| 14 | wastewater metagenome (하수) | 312,695 | 0.8% |
| 15 | Mycobacterium tuberculosis (결핵) | 291,729 | 0.7% |

> SARS-CoV-2가 전체 샘플의 23%로 1위 — 팬데믹 기간 대규모 게놈 감시의 결과.
> 메타게놈(토양, 장내, 해양, 하수 등) 합산 시 전체의 ~10%.

### 5-5. 연구 유형 (Study Type)

| 연구 유형 | 건수 | 비율 |
|-----------|------|------|
| Other | 392,632 | 55.8% |
| Whole Genome Sequencing | 122,328 | 17.4% |
| Transcriptome Analysis | 117,936 | 16.8% |
| Metagenomics | 68,647 | 9.8% |
| Population Genomics | 826 | 0.1% |
| Epigenetics | 688 | 0.1% |
| Cancer Genomics | 428 | 0.1% |
| Exome Sequencing | 366 | 0.1% |

### 5-6. 등록 기관 (Center Name) TOP 10

| 기관 | 연구 수 | 비율 |
|------|---------|------|
| BioProject | 432,963 | 61.5% |
| GEO | 152,307 | 21.6% |
| Wellcome Sanger Institute | 12,401 | 1.8% |
| JCVI | 8,872 | 1.3% |
| EBI | 2,850 | 0.4% |
| DOE-JGI | 2,643 | 0.4% |
| UMIGS | 2,568 | 0.4% |
| JGI | 2,397 | 0.3% |
| dbGaP | 1,827 | 0.3% |
| WUGSC | 1,599 | 0.2% |

> 61.5%가 BioProject 경유 등록, 21.6%가 GEO 경유 등록.

### 5-7. ID 매핑 현황

| 매핑 | 건수 |
|------|------|
| SRA_Run → SRA_Experiment | 41,237,472 |
| SRA_Experiment → SRA_Sample | 38,851,031 |
| SRA_Experiment → SRA_Study | 38,850,956 |

---

## 6. 데이터 품질 요약

| 항목 | 상태 | 비고 |
|------|------|------|
| 파싱 에러 | **0건** | 전체 1.2억 레코드 무결 |
| Study 초록 누락 | 6.88% | 일부 연구는 초록 미등록 |
| Sample 제목 누락 | 26.82% | 제목 없이 accession만 등록된 샘플 다수 |
| Run 정량 데이터 | **100% 누락** | FTP 덤프에 미포함, API 보완 필요 |
| 핵심 식별자 (accession) | 100% 완전 | 모든 테이블 PK 누락 없음 |
| 실험 메타데이터 | 91%+ 완전 | strategy, layout, instrument 등 |

---

## 7. 보완이 필요한 항목

| 항목 | 현재 상태 | 보완 방법 | 우선순위 |
|------|-----------|-----------|----------|
| Run 정량 데이터 (reads, bases, size) | 100% NULL | `SRA_Accessions.tab` (30GB) 또는 E-utilities API | 높음 |
| GEO 고유 메타데이터 (GSE/GSM/GPL) | 미수집 | GEO FTP MINiML 파싱 (Phase 2) | 중간 |
| BioSample 상세 속성 | external_ids만 확보 | BioSample XML 수집 | 낮음 |
| PubMed 논문 정보 | ID만 확보 | PubMed efetch | 낮음 |

---

## 8. 파일 산출물

| 파일 | 크기 | 내용 |
|------|------|------|
| `data/downloads/sra/NCBI_SRA_Metadata_Full_20260316.tar.gz` | 15.1GB | 원본 SRA 메타데이터 덤프 |
| `data/metadata_crawl.sqlite` | 66GB | 파싱 완료된 SQLite DB |
| `data/export/sra_studies.json` | 617MB | Study 테이블 JSON 내보내기 |
| `data/export/sra_samples.json` | (생성 중) | Sample 테이블 JSON 내보내기 |
| `data/export/sra_experiments.json` | (생성 중) | Experiment 테이블 JSON 내보내기 |
| `data/export/sra_runs.json` | (생성 중) | Run 테이블 JSON 내보내기 |
| `data/logs/qc_report.json` | - | QC 리포트 (필드별 NULL 통계) |
