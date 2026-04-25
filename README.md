# `streaming_sbdf_rs`

`streaming_sbdf_rs`는 Rust 기반 SBDF writer와 Parquet -> SBDF 변환 helper를 제공하는 Python 패키지다.

이 디렉터리가 실제 editable 설치 루트다.

## 현재 구조

- `pyproject.toml`: Python 패키지 메타데이터
- `Cargo.toml`: Rust crate 설정
- `src/streaming_sbdf_rs/__init__.py`: Python 래퍼
- `src/lib.rs`: Rust 구현
- `vendor/`: 포함된 외부 소스

## 공개 API

- `StreamingSbdfWriter`
- `SBDFError`
- `parquet_to_sbdf_streaming()`

## 주요 기능

- Rust 기반 SBDF writer
- Parquet 파일을 batch 단위로 읽어 SBDF로 스트리밍 변환
- 출력 경로 부모 디렉터리 자동 생성

## 설치

```bash
pip install -e .
```

Rust 확장을 빌드해야 하므로 Rust toolchain이 필요하다.

자세한 호출 예시는 [USAGE.md](./USAGE.md) 를 보면 된다.
