#### rust_streaming_sbdf Build Guide

이 문서는 `rust_streaming_sbdf` 저장소 루트를 기준으로,
`streaming_sbdf_rs` 패키지를 빌드하고 설치하는 흐름을 설명한다.

이 문서의 모든 상대경로는 현재 작업 디렉토리가 저장소 루트일 때를 기준으로 한다.

핵심 원칙은 이렇다.

- Rust 코어만 따로 배포하지 않는다.
- Python 래퍼는 Rust 코어 호출을 얇게 감싸는 역할만 한다.
- 최종 사용자는 `pip install <wheel>` 한 번으로 Rust 확장과 Python 래퍼를 같이 사용한다.

### 저장소 구조

- `Cargo.toml`
  - Rust 확장 모듈 메타데이터
- `pyproject.toml`
  - `maturin` 빌드 진입점
- `build.rs`
  - vendored `sbdf-c` 정적 빌드 설정
- `src/`
  - Rust 구현
- `python/streaming_sbdf_rs/__init__.py`
  - Python public API
- `vendor/sbdf-c/`
  - SBDF 포맷 처리를 위한 vendored C 라이브러리

즉 실제 배포 단위는 아래 하나다.

- Python package: `streaming_sbdf_rs`

이 패키지 안에 다음이 같이 들어간다.

- compiled Rust extension
- Python wrapper
- SBDF writer public API
- Parquet -> SBDF streaming helper

현재 기준의 기본 진입점은 아래다.

- `streaming_sbdf_rs.StreamingSbdfWriter`
- `streaming_sbdf_rs.SBDFError`
- `streaming_sbdf_rs.parquet_to_sbdf_streaming`

역할 분리는 아래처럼 본다.

- `streaming_sbdf_rs`
  - 저수준 SBDF writer
  - Parquet 입력 helper
  - batch 단위 dict-of-lists 를 받아 SBDF slice 를 기록한다.
  - Rust Arrow/Parquet reader로 Parquet를 읽고 batch 단위로 SBDF writer에 넘긴다.

### 왜 한 패키지로 묶는가

개발 중에는 editable 형태로 설치하고 로컬에서 바로 테스트할 수 있다.
하지만 배포와 재사용 관점에서는 한 패키지로 묶는 편이 더 안전하다.

장점:

- 설치가 단순하다.
- Rust 바이너리와 Python 코드 버전이 같이 맞춰진다.
- 사용자 입장에서 `import streaming_sbdf_rs` 하나로 끝난다.
- `pj-etl_system` 쪽은 Rust 소스 위치를 몰라도 된다.

### 빌드 전 준비

예시는 상위 프로젝트 가상환경 `../pj-etl_system/.venv` 를 사용한다.

필수 준비물:

- Rust toolchain
- Python virtualenv
- `maturin`
- `pip`

`maturin` 설치:

```bash
../pj-etl_system/.venv/bin/pip install maturin
```

### 개발용 설치

개발 중에는 editable 형태로 설치하는 것이 편하다.

```bash
../pj-etl_system/.venv/bin/python -m maturin develop
```

이 명령을 실행하면:

- Rust 확장을 컴파일한다.
- 현재 Python 환경에 editable 형태로 설치한다.
- `import streaming_sbdf_rs` 로 바로 테스트할 수 있다.

적합한 경우:

- Rust 코드를 자주 수정할 때
- 로컬 기능 테스트를 빠르게 반복할 때

### 배포용 wheel 빌드

배포하거나 다른 환경에 넘길 때는 wheel을 만든다.

```bash
../pj-etl_system/.venv/bin/python -m maturin build --release
```

빌드가 끝나면 wheel은 보통 아래에 생성된다.

- `target/wheels/`

예시 산출물:

- `target/wheels/streaming_sbdf_rs-0.1.0-cp314-cp314-manylinux_2_34_x86_64.whl`

### wheel 설치

빌드된 wheel은 일반 Python 패키지처럼 설치한다.

```bash
../pj-etl_system/.venv/bin/pip install --force-reinstall \
  target/wheels/streaming_sbdf_rs-0.1.0-cp314-cp314-manylinux_2_34_x86_64.whl
```

이후 같은 Python 환경에서 바로 아래 import가 가능해야 한다.

```python
import streaming_sbdf_rs
from streaming_sbdf_rs import SBDFError, StreamingSbdfWriter
```

### 상위 프로젝트에서 사용하는 방식

`pj-etl_system` 에서는 Rust 소스를 직접 import하지 않는다.
반드시 가상환경에 설치된 wheel 또는 editable install 결과를 사용한다.

예시:

```python
from streaming_sbdf_rs import parquet_to_sbdf_streaming

parquet_to_sbdf_streaming(
    "/path/to/input.parquet",
    "/path/to/output.sbdf",
    batch_size=50_000,
)
```

이 helper의 동작 기준:

- `parquet_path`, `sbdf_path` 는 `str | pathlib.Path` 를 받을 수 있다.
- `column_types` 를 생략하면 Rust Arrow/Parquet reader가 해석한 Parquet 스키마를 기준으로 Spotfire 타입을 자동 매핑한다.
- helper는 DuckDB에 의존하지 않고, Parquet 파일을 직접 읽어 row batch 단위로 스트리밍 export 한다.
- 중첩 Parquet 타입(`ARRAY`, `LIST`, `STRUCT`, `MAP`, `...[]`)은 지원하지 않으며, 컬럼명과 Arrow 타입을 포함한 예외를 발생시킨다.
- 중첩 타입이 필요하면 upstream 단계에서 scalar 타입으로 미리 변환한 parquet를 넘겨야 한다.

또는 Rust 패키지를 직접 사용할 수도 있다.

```python
from pathlib import Path
from streaming_sbdf_rs import StreamingSbdfWriter

path = Path("/tmp/example.sbdf")
writer = StreamingSbdfWriter(
    str(path),
    columns=["id", "name"],
    column_types={"id": "LongInteger", "name": "String"},
)
writer.write_batch({"id": [1, 2], "name": ["a", None]})
writer.close()
```

저수준 writer 사용 시 주의:

- 현재 생성자는 파일 경로를 문자열로 받는 것을 기준으로 사용한다.
- `Path` 객체를 쓴다면 `str(path)` 로 넘기는 편이 안전하다.
- 자동 Parquet 타입 추론은 이 writer가 아니라 상위 helper에서 수행한다.

### 테스트

빠른 수동 확인 예시는 아래처럼 할 수 있다.

```bash
../pj-etl_system/.venv/bin/python - <<'PY'
from pathlib import Path
from streaming_sbdf_rs import StreamingSbdfWriter

path = Path("/tmp/streaming_sbdf_rs_test.sbdf")
writer = StreamingSbdfWriter(
    str(path),
    columns=["id", "name"],
    column_types={"id": "LongInteger", "name": "String"},
)
writer.write_batch({"id": [1, 2], "name": ["a", None]})
writer.close()
print(path.exists(), path.stat().st_size)
PY
```

확인하는 내용:

- Rust 확장이 import 되는지
- batch write 가 정상 동작하는지
- SBDF 파일이 실제로 생성되는지

### 권장 운영 방식

- 로컬 개발:
  - `maturin develop`
- 배포/공유:
  - `maturin build --release`
  - `pip install <wheel>`

즉 결론은 이렇다.

- 개발 단계에서는 editable 설치
- 배포 단계에서는 wheel 설치
- 기준 저장소 루트는 `rust_streaming_sbdf`
- 상위 소비 프로젝트는 설치된 `streaming_sbdf_rs` 만 import
