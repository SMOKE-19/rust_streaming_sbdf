# `streaming_sbdf_rs` 사용법

이 문서는 외부 사용자가 `streaming_sbdf_rs` 패키지를 import 해서 사용하는 기준으로 설명한다.

## 설치

```bash
pip install -e .
```

Rust 확장 빌드 환경이 필요하다.

## 가장 간단한 진입점

대부분은 `parquet_to_sbdf_streaming()` 으로 시작하면 된다.

```python
from streaming_sbdf_rs import parquet_to_sbdf_streaming

output_path = parquet_to_sbdf_streaming(
    parquet_path="input.parquet",
    sbdf_path="out/output.sbdf",
)

print(output_path)
```

## 여러 Parquet 파일 변환

dataset shard처럼 여러 parquet 파일을 하나의 SBDF로 이어 쓰려면 `parquet_files_to_sbdf_streaming()` 을 사용한다.

```python
from streaming_sbdf_rs import parquet_files_to_sbdf_streaming

parquet_files_to_sbdf_streaming(
    parquet_files=[
        "snapshot/part-000.parquet",
        "snapshot/part-001.parquet",
        "snapshot/part-002.parquet",
    ],
    sbdf_path="out/snapshot.sbdf",
)
```

파일은 전달한 순서 그대로 처리된다. 첫 파일의 schema를 기준으로 나머지 파일은 strict match 되어야 하며, 불일치하면 문제 파일 경로가 포함된 예외가 발생한다.

## Dataset 디렉터리 변환

디렉터리의 `.parquet` 파일을 정렬된 순서로 처리하려면 `parquet_dataset_to_sbdf_streaming()` 을 사용한다.

```python
from streaming_sbdf_rs import parquet_dataset_to_sbdf_streaming

parquet_dataset_to_sbdf_streaming(
    dataset_path="snapshot",
    sbdf_path="out/snapshot.sbdf",
    recursive=False,
)
```

하위 디렉터리까지 포함해야 하면 `recursive=True` 를 지정한다. 운영 재현성이 중요하면 디렉터리 자동 탐색보다 명시적인 file list나 manifest 사용을 권장한다.

## Manifest 변환

line-based manifest 파일로 입력 순서를 고정할 수도 있다.

```text
# snapshot.manifest
snapshot/part-000.parquet
snapshot/part-001.parquet
snapshot/part-002.parquet
```

```python
from streaming_sbdf_rs import parquet_manifest_to_sbdf_streaming

parquet_manifest_to_sbdf_streaming(
    manifest_path="snapshot.manifest",
    sbdf_path="out/snapshot.sbdf",
)
```

manifest의 상대 경로는 manifest 파일이 있는 디렉터리를 기준으로 해석된다. 빈 줄과 `#` 로 시작하는 줄은 무시된다.

## 주요 옵션

- `batch_size`: 기본값 `50000`
- `column_types`: 컬럼별 타입 강제 지정용 dict
- `encoding_rle`: 반복값 압축 사용 여부. 기본값 `True`

예시:

```python
from streaming_sbdf_rs import parquet_to_sbdf_streaming

parquet_to_sbdf_streaming(
    parquet_path="input.parquet",
    sbdf_path="out/output.sbdf",
    batch_size=100000,
    column_types={"price": "Real", "name": "String"},
    encoding_rle=True,
)
```

## 저수준 API

더 직접 제어가 필요하면 아래 타입을 사용할 수 있다.

- `StreamingSbdfWriter`
- `SBDFError`

```python
from streaming_sbdf_rs import SBDFError, StreamingSbdfWriter
```

## 반환값

변환 helper는 최종 `sbdf_path` 를 `Path` 로 반환한다.

## 주의사항

- 출력 파일의 부모 디렉터리가 없으면 자동으로 생성한다.
- 다중 parquet 입력은 writer를 재초기화하지 않고 하나의 SBDF stream으로 이어 쓴다.
- 빈 file list나 빈 dataset은 명시적으로 실패한다.
- 실제 지원 타입과 세부 동작은 Rust 구현에 따라 결정된다.
