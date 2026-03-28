# Troubleshoot Log

Claude가 작업 중 발생시킨 실수, 에러, 디버깅 과정을 기록한다.
향후 같은 실수를 반복하지 않기 위한 참고 자료.

---

## [빌드] hatchling 패키지 탐색 실패 (A, B, C 공통)
- **증상**: `uv sync` 시 "Unable to determine which files to ship inside the wheel"
- **원인**: hatchling이 프로젝트명과 동일한 디렉토리를 기본 탐색하는데, `src/` 레이아웃과 불일치
- **LLM 실수 원인 추론**:
  1. 컨텍스트 부족 — hatchling의 src layout 기본 동작을 정확히 모름
  2. 관습 의존 — setuptools의 `src/` layout 자동 탐색 관습을 hatchling에도 적용
- **해결**: `[tool.hatch.build.targets.wheel] packages = ["src"]` 추가
- **교훈**: hatchling은 setuptools와 다르게 src layout 자동 탐색 안 함 — 명시적 설정 필수

## [호환성] SQLite에서 JSONB 미지원 (A, B, C 공통)
- **증상**: 테스트 시 `UnsupportedCompilationError: can't render element of type JSONB`
- **원인**: PostgreSQL 전용 JSONB 타입을 SQLite in-memory 테스트에서 사용
- **LLM 실수 원인 추론**:
  1. 테스트 환경 미고려 — 개발 시 PostgreSQL 기준으로 설계하고 테스트 환경(SQLite) 차이를 간과
  2. 과신 — SQLAlchemy가 자동으로 타입 변환해줄 것으로 가정
- **해결**: PortableJSON/FlexibleJSON TypeDecorator로 dialect별 분기 (PG→JSONB, SQLite→JSON)
- **교훈**: 다중 DB 백엔드 지원 시 dialect-specific 타입은 반드시 추상화 필요

## [파서] SRA XML SAMPLE_ATTRIBUTE 파싱 실패 (접근법 A)
- **증상**: SRA 샘플 속성이 빈 dict로 파싱됨
- **원인**: SRA XML은 `<TAG>text</TAG><VALUE>text</VALUE>` 자식 엘리먼트 패턴인데 XML attribute로만 조회
- **LLM 실수 원인 추론**:
  1. 스키마 미확인 — SRA XML 스키마를 사전 확인하지 않고 일반적인 XML attribute 패턴 가정
  2. 테스트 데이터 부재 — 실제 SRA XML 샘플 없이 구현
- **해결**: 자식 엘리먼트 우선 탐색 후 attribute fallback으로 수정
- **교훈**: SRA XML의 TAG/VALUE 패턴은 attribute가 아닌 child element — 스키마 먼저 확인
