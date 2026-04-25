# 변경 이력

## 2026-04-25 dataset/file-list 입력 지원

### 요약

- 단일 parquet 입력 API는 유지하면서 여러 parquet shard를 하나의 SBDF stream으로 이어 쓰는 경로를 추가했다.
- Rust 바인딩에 `parquet_files_to_sbdf_streaming()`을 추가하고, Python wrapper에 file list, dataset 디렉터리, manifest helper를 공개했다.
- 첫 parquet 파일 schema를 기준으로 나머지 파일을 strict match 검사하도록 했다.
- shard 처리 중 오류가 나면 관련 parquet 경로를 포함해 예외를 반환하도록 했다.

### 변경 파일

- `src/lib.rs`: 다중 parquet 처리 루프, schema 검증, 공통 변환 구현 추가
- `src/streaming_sbdf_rs/__init__.py`: `parquet_files_to_sbdf_streaming()`, `parquet_dataset_to_sbdf_streaming()`, `parquet_manifest_to_sbdf_streaming()` 추가
- `README.md`: 공개 API와 주요 기능 목록 갱신
- `USAGE.md`: file list, dataset, manifest 사용 예시와 정책 문서화
- `.TODO/DATASET_INPUT_SUPPORT.md`: 구현 로드맵 체크 및 결과 요약 추가

### 검증

- `cargo fmt --check`
- `cargo test`
- `cargo clippy -- -D warnings`
- `python -m compileall -q src/streaming_sbdf_rs`

### 남은 항목

- 실제 대용량 shard fixture를 사용한 통합 테스트 추가
