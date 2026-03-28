# Changelog

프로젝트 작업 이력을 기록한다.

## 2026-03-25 — 세션 시작

### 초기화
- 워크스페이스 초기화 완료
- 작업 의뢰서(`제목없음.md`) 기반으로 프로젝트 목표 설정

### Phase 0
- 의뢰 자료 1건 수집, 자동 보완 4항목 (실행환경, DB, 성공기준, 예외처리)

### Phase 1
- 접근법 3가지 도출: A(FTP Bulk+Async), B(API+pysradb/GEOparse), C(Hybrid+Prefect)
- 사용자 선택: 전부 (3개 모두 병렬 구현)

### Phase 2
- 3개 병렬 에이전트 실행 시작 (worktree 격리)
- 접근법 A (FTP Bulk+Async): 테스트 57/57 통과, Docker 580MB — lxml iterparse 스트리밍 기반
- 접근법 B (API+pysradb/GEOparse): 테스트 33/33 통과, Docker 848MB — 라이브러리 래핑 기반
- 접근법 C (Hybrid+Prefect): 테스트 32/32 통과, Docker 1.01GB — Prefect 오케스트레이션 기반

### Phase 3
- 비교 리포트 작성 — A 추천 (전수 수집 최적, 가장 가벼움, 최다 테스트)
- 사용자 최종 선택: 접근법 A (FTP Bulk+Async)

### Phase 4
- 접근법 A worktree 코드를 메인 프로젝트에 복사
- 메인 프로젝트에서 테스트 재확인: 57/57 통과
- CLAUDE.md 최종 확정 (아키텍처, 기술 스택, 컨벤션)
- report.md 최종 리포트 생성

### 추가 구현
- 구현: GEO FTP 전체 GSE 자동 탐색 (`list_all_gse_accessions`) — 수동 --gse 지정 없이 전수 수집 가능
- 구현: SQLiteWriter — PostgreSQL 없이도 로컬 SQLite 파일에 영구 저장 (자동 fallback)
- 구현: JSON 내보내기 (`--export-json`) — SQLite 데이터를 테이블별 JSON 파일로 출력
- 테스트: 57 → 64개로 확장 (GSE 탐색 2개 + SQLiteWriter 5개)
