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

## 주요 옵션

- `batch_size`: 기본값 `50000`
- `column_types`: 컬럼별 타입 강제 지정용 dict

예시:

```python
from streaming_sbdf_rs import parquet_to_sbdf_streaming

parquet_to_sbdf_streaming(
    parquet_path="input.parquet",
    sbdf_path="out/output.sbdf",
    batch_size=100000,
    column_types={"price": "Real", "name": "String"},
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

`parquet_to_sbdf_streaming()` 은 최종 `sbdf_path` 를 `Path` 로 반환한다.

## 주의사항

- 출력 파일의 부모 디렉터리가 없으면 자동으로 생성한다.
- 실제 지원 타입과 세부 동작은 Rust 구현에 따라 결정된다.
