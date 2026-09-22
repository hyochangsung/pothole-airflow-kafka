# AGENTS.md

이 문서는 이 저장소에서 작업하는 AI 에이전트와 개발자를 위한 작업 기준이다.

## 작업 원칙

- 기존 파일과 현재 `git status`를 먼저 확인하고, 사용자 변경과 무관한 파일은 수정하지 않는다.
- `.env`, API 키, 데이터베이스 비밀번호, AWS 자격 증명은 만들거나 커밋하지 않는다.
- 실제 AWS, RDS, S3, Kafka 상태를 바꾸는 명령은 실행 전에 대상과 영향을 확인한다.

## 확인 명령

변경 범위에 맞는 최소 검증을 실행한다.

```bash
# Compose 문법과 환경 변수 치환 결과 확인
docker compose config

# Python 문법 확인
python3 -m compileall consumer consumer_rds consumer_s3 airflow/dags

# 필요한 경우 서비스 재빌드 및 실행
docker compose up --build

# 특정 서비스 로그 확인
docker compose logs --follow raw-log-s3-consumer
docker compose logs --follow raw-log-rds-consumer
```

외부 서비스 자격 증명이 없는 환경에서는 `docker compose config`와 변경한 Python 파일의 문법 확인까지 수행하고, 실행하지 못한 통합 검증은 PR에 기록한다.

## GitHub 작업 흐름

저장소에 남길 변경은 아래 순서로 진행한다.

1. GitHub Issue를 만들거나 기존 Issue를 확인한다.
2. Issue 번호를 포함한 작업 브랜치를 만든다. 예: `docs/1-agent-guidelines`.
3. 변경을 구현하고 범위에 맞는 검증을 실행한다.
4. 하나의 목적을 담은 커밋을 만든다.
5. Issue를 연결한 Pull Request를 만든다.
6. PR에서 변경 내용과 CI 결과를 확인한 뒤 병합한다.
7. PR 본문에 `Closes #번호`를 작성해 병합 시 Issue가 닫히게 한다.

GitHub 작업은 `gh` CLI를 사용한다. PR 제목과 본문은 한국어로 작성한다. PR을 만들기 전에는 `git diff --check`, `git status`, 실행한 검증 결과를 확인한다.

## AI 스킬

- `.agents/skills/grill-me/`는 계획이나 설계를 강하게 점검할 때 사용한다. 사용자가 "이 계획을 grill-me로 검토해 줘"처럼 명시적으로 요청할 때만 시작한다.
- `grill-me`는 `.agents/skills/grilling/`을 호출한다. `grilling`은 한 번에 답할 수 있는 결정만 질문하고, 사용자의 답에 따라 다음 질문을 이어 간다. 합의에 도달하기 전에는 구현을 시작하지 않는다.
- 스킬은 반복 가능한 작업 절차를 제공하고, 이 문서는 저장소 전체에 적용되는 규칙과 검증 기준을 제공한다.
