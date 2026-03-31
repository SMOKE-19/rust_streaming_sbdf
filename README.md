# rust_streaming_sbdf

Rust/PyO3 기반 SBDF 스트리밍 writer 패키지입니다.

실제 배포 패키지 이름은 `streaming_sbdf_rs` 이고, `pj-etl_system` 에서는
가상환경에 설치된 이 패키지를 import 해서 사용합니다.

빌드와 설치 방법은 [`build.md`](/home/smoke_nb_sv/dev/projects/rust_streaming_sbdf/build.md) 를 참고하면 됩니다.

핵심 사용 기준:

- `streaming_sbdf_rs` 는 저수준 SBDF writer와 고수준 Parquet export helper를 함께 제공합니다.
- `streaming_sbdf_rs.parquet_to_sbdf_streaming(...)` 는 `Path` 입력을 받을 수 있습니다.
- helper에서 `column_types` 를 생략하면 Rust Arrow/Parquet reader가 해석한 Parquet 스키마를 기준으로 Spotfire 타입을 자동 매핑합니다.
- helper는 DuckDB에 의존하지 않고, Parquet 파일 경로를 직접 읽어 row batch 단위로 스트리밍 export 합니다.
- 중첩 Parquet 타입(`ARRAY`, `LIST`, `STRUCT`, `MAP`, `...[]`)은 SBDF export 대상으로 지원하지 않으며, 명시적 예외를 발생시킵니다.

저수준 writer 사용 기준:

- `StreamingSbdfWriter` 는 현재 출력 파일 경로를 문자열로 받는 것을 기준으로 사용합니다.
- `StreamingSbdfWriter` 를 직접 사용할 때는 `column_types` 를 명시하는 것을 권장합니다.

라이선스는 top-level [`LICENSE`](/home/smoke_nb_sv/dev/projects/rust_streaming_sbdf/LICENSE) 를 따르며,
vendored `sbdf-c` 관련 고지는 [`THIRD_PARTY_NOTICES.md`](/home/smoke_nb_sv/dev/projects/rust_streaming_sbdf/THIRD_PARTY_NOTICES.md)
에 정리되어 있습니다.
