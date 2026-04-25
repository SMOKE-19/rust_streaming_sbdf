# dataset/file-list 입력 지원 TODO

이 문서는 `rust_streaming_sbdf`가 현재의 단일 parquet 입력 API를 넘어, parquet dataset 또는 parquet file list를 직접 받아 SBDF로 안정적으로 변환할 수 있도록 확장하는 방향을 정리한 내부 메모다.

## 배경

현재 공개 API인 `parquet_to_sbdf_streaming()` 은 단일 `parquet_path` 만 받는다.

- 구현 위치: [src/streaming_sbdf_rs/__init__.py](../src/streaming_sbdf_rs/__init__.py)
- Rust 바인딩 구현: [src/lib.rs](../src/lib.rs)

이 구조는 단일 parquet export에는 단순하고 안정적이지만, upstream ETL이 대용량 결과를 dataset shard 형태로 내리는 경우에는 중간에 다시 단일 parquet로 합치는 단계가 필요해진다.

특히 `smoking_data`의 ETL 03.01 snapshot 같은 흐름에서는 아래 요구가 생긴다.

- 대용량 snapshot을 dataset shard 형태로 기록
- downstream SBDF export는 이 shard들을 순차 소비
- 중간의 단일 대용량 parquet 재물질화는 피하기

## 현재 한계

- `parquet_to_sbdf_streaming(parquet_path, sbdf_path, ...)` 형태만 지원
- parquet dataset 디렉터리 입력 미지원
- parquet file list 입력 미지원
- manifest 파일을 통한 다중 parquet 입력 미지원
- 입력 파일 순서 제어는 호출자 쪽 책임

## 목표 방향

다음 중 최소 하나를 공식 지원하는 것이 좋다.

1. parquet file list 입력
2. parquet dataset 디렉터리 입력
3. manifest 파일 입력

현실적인 우선순위는 `file list -> dataset 디렉터리 -> manifest` 순서가 적절해 보인다.

이유:

- file list가 가장 단순하고 upstream 제어력이 높다
- dataset 디렉터리는 사용자 편의가 좋다
- manifest는 재현성과 명시적 순서 제어에 유리하다

## 권장 API 스케치

### 1차

기존 API는 유지하고, 다중 입력용 함수를 추가한다.

```python
parquet_files_to_sbdf_streaming(
    parquet_files: list[str | Path],
    sbdf_path: str | Path,
    *,
    batch_size: int = 50_000,
    column_types: dict[str, str] | None = None,
) -> Path
```

### 2차

dataset 디렉터리 입력용 helper를 추가한다.

```python
parquet_dataset_to_sbdf_streaming(
    dataset_path: str | Path,
    sbdf_path: str | Path,
    *,
    batch_size: int = 50_000,
    column_types: dict[str, str] | None = None,
    recursive: bool = False,
) -> Path
```

### 3차

manifest 기반 입력을 추가해 호출자 쪽 순서 제어를 더 명확히 한다.

```python
parquet_manifest_to_sbdf_streaming(
    manifest_path: str | Path,
    sbdf_path: str | Path,
    *,
    batch_size: int = 50_000,
    column_types: dict[str, str] | None = None,
) -> Path
```

## 구현 원칙

- 기존 단일 parquet API는 호환성 때문에 유지
- 내부적으로는 "입력 source iterator -> record batch reader -> SBDF writer" 구조로 일반화
- 각 입력 parquet는 순차 소비
- 입력 간 schema 불일치 정책을 명확히 정의
- 파일 순서는 호출자가 넘긴 순서를 기본으로 존중
- dataset 디렉터리 입력은 파일명 정렬보다 명시적 정렬 규약 또는 manifest 우선 정책을 권장

## 세부 고려사항

### schema 정합성

- 가장 단순한 정책은 "첫 파일 schema 기준 + 나머지 파일 strict match"
- 이후 필요하면 `union_by_name` 성격의 느슨한 모드 추가 검토

### 파일 순서

- file list 입력은 주어진 순서를 그대로 사용
- dataset 디렉터리 입력은 정렬 규약을 문서화해야 함
- upstream이 parquet metadata 기반으로 순서를 정했다면, 그 결과를 file list나 manifest로 넘기는 쪽이 가장 명확함

### 에러 처리

- 중간 shard 실패 시 어떤 파일에서 실패했는지 경로를 포함해 반환
- 빈 입력 list나 빈 dataset은 명시적으로 실패시킬지, 빈 SBDF를 허용할지 정책 결정 필요

### 성능

- 입력 파일 사이에서 writer 재초기화 없이 하나의 SBDF stream으로 이어쓰기
- batch 크기는 전체 공통 옵션으로 유지
- shard 간 경계는 사용자에게 숨기고, 논리적으로는 하나의 테이블로 export

## 구현 로드맵

- [x] 현재 `parquet_to_sbdf_streaming()` 내부 흐름을 "단일 파일 전용"에서 "여러 parquet source 순차 처리" 구조로 일반화할 수 있는 경계를 식별
- [x] Python 래퍼와 Rust 바인딩 양쪽에 `parquet_files_to_sbdf_streaming()` API를 추가
- [x] Rust 쪽에서 여러 parquet 파일을 순차적으로 열고 `RecordBatch`를 이어서 SBDF writer로 보내는 루프 구현
- [x] 다중 입력 시 schema 정합성 검사 정책을 정의하고 에러 메시지에 문제 파일 경로를 포함
- [x] Python 레벨에서 dataset 디렉터리를 file list로 해석하는 helper를 추가
- [x] manifest 입력 필요 여부를 검토하고, 필요하면 JSON 또는 line-based text 형식 규약을 추가
- [x] README와 USAGE에 "단일 parquet"와 "dataset/file list" 사용 예시를 함께 문서화
- [ ] 대용량 shard 여러 개를 순차 export 하는 통합 테스트를 추가

## 구현 결과

- `parquet_files_to_sbdf_streaming()` 추가: 호출자가 넘긴 parquet file list 순서를 그대로 사용한다.
- `parquet_dataset_to_sbdf_streaming()` 추가: 디렉터리의 `.parquet` 파일을 정렬해 file list helper로 위임하며 `recursive` 옵션을 지원한다.
- `parquet_manifest_to_sbdf_streaming()` 추가: line-based manifest를 지원하고 빈 줄과 `#` 주석을 무시한다.
- Rust 구현은 첫 parquet 파일의 schema를 기준으로 이후 파일을 strict match 검사한다.
- 빈 file list 또는 빈 dataset은 Rust 바인딩에서 명시적으로 실패한다.
- 검증은 `cargo fmt --check`, `cargo test`, `cargo clippy -- -D warnings`, `python -m compileall -q src/streaming_sbdf_rs`로 수행했다.

## 메모

- upstream ETL이 이미 parquet metadata 기반으로 파일 리스트 순서를 조정하고 있을 수 있으므로, `rust_streaming_sbdf`는 그 순서를 그대로 받아 처리할 수 있는 API를 제공하는 것이 가장 유연하다.
- dataset 디렉터리 직접 지원은 편하지만, 운영 재현성과 명시적 순서 제어 측면에서는 file list 또는 manifest 입력이 더 안전할 수 있다.
