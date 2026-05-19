from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import socketserver


PORT = 7001


@dataclass(frozen=True)
class Measurement:
    sensor_id: str
    timestamp: str
    data_type: str
    value: float


@dataclass(frozen=True)
class TypeAnalysis:
    sensor_id: str
    data_type: str
    count: int
    average: float
    maximum: float
    high_risk_count: int
    risk_level: str


@dataclass(frozen=True)
class AnalysisResult:
    gateway_id: str
    generated_at: str
    total_records: int
    average_value: str
    max_value: str
    risk_level: str
    summary: str


class ThreadingTcpServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


def read_protocol_line(raw_line: bytes) -> str:
    return raw_line.decode("utf-8-sig").strip()


def format_number(value: float) -> str:
    text = f"{value:.2f}".rstrip("0").rstrip(".")
    return "0" if text == "-0" else text


def parse_measurement(line: str) -> Measurement | None:
    parts = line.split("|")
    if len(parts) < 5 or parts[0].upper() != "DATA":
        return None

    try:
        value = float(parts[4].strip().replace(",", "."))
    except ValueError:
        return None

    return Measurement(
        sensor_id=parts[1].strip(),
        timestamp=parts[2].strip(),
        data_type=parts[3].strip().upper(),
        value=value,
    )


def is_high_risk(measurement: Measurement) -> bool:
    if measurement.data_type == "TEMP":
        return measurement.value >= 40
    if measurement.data_type == "HUM":
        return measurement.value >= 90
    if measurement.data_type == "RUIDO":
        return measurement.value >= 85
    if measurement.data_type == "PM2.5":
        return measurement.value >= 35
    if measurement.data_type == "PM10":
        return measurement.value >= 50
    if measurement.data_type == "AQ":
        return measurement.value >= 150
    return False


def is_medium_risk(data_type: str, average: float, maximum: float) -> bool:
    if data_type == "TEMP":
        return maximum >= 35 or average >= 30
    if data_type == "HUM":
        return maximum >= 80 or average >= 75
    if data_type == "RUIDO":
        return maximum >= 75 or average >= 65
    if data_type == "PM2.5":
        return maximum >= 20 or average >= 12
    if data_type == "PM10":
        return maximum >= 35 or average >= 25
    if data_type == "AQ":
        return maximum >= 100 or average >= 75
    return False


def analyze_sensor_type(sensor_id: str, data_type: str, measurements: list[Measurement]) -> TypeAnalysis:
    count = len(measurements)
    average = sum(item.value for item in measurements) / count if count else 0
    maximum = max((item.value for item in measurements), default=0)
    high_risk_count = sum(1 for item in measurements if is_high_risk(item))
    risk_level = (
        "ALTO"
        if high_risk_count > 0
        else "MEDIO"
        if is_medium_risk(data_type, average, maximum)
        else "BAIXO"
    )

    return TypeAnalysis(sensor_id, data_type, count, average, maximum, high_risk_count, risk_level)


def analyze(gateway_id: str, measurements: list[Measurement]) -> AnalysisResult:
    groups: dict[tuple[str, str], list[Measurement]] = {}
    for measurement in measurements:
        key = (measurement.sensor_id, measurement.data_type)
        groups.setdefault(key, []).append(measurement)

    type_analyses = [
        analyze_sensor_type(sensor_id, data_type, groups[(sensor_id, data_type)])
        for sensor_id, data_type in sorted(groups)
    ]

    high_risk_count = sum(item.high_risk_count for item in type_analyses)
    medium_risk_count = sum(1 for item in type_analyses if item.risk_level == "MEDIO")
    risk_level = "ALTO" if high_risk_count > 0 else "MEDIO" if medium_risk_count > 0 else "BAIXO"

    average_by_type = (
        "; ".join(f"{item.sensor_id}/{item.data_type}={format_number(item.average)}" for item in type_analyses)
        if type_analyses
        else "-"
    )
    max_by_type = (
        "; ".join(f"{item.sensor_id}/{item.data_type}={format_number(item.maximum)}" for item in type_analyses)
        if type_analyses
        else "-"
    )
    summary = (
        "; ".join(
            f"{item.sensor_id}/{item.data_type}:n={item.count},risco={item.risk_level},alertas={item.high_risk_count}"
            for item in type_analyses
        )
        if type_analyses
        else "sem_registos"
    )

    return AnalysisResult(
        gateway_id=gateway_id,
        generated_at=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        total_records=len(measurements),
        average_value=average_by_type,
        max_value=max_by_type,
        risk_level=risk_level,
        summary=summary,
    )


class AnalysisHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        try:
            header = read_protocol_line(self.rfile.readline())
            if not header:
                return

            header_parts = header.split("|")
            if len(header_parts) < 3 or header_parts[0].upper() != "ANALYZE_BATCH":
                self.write_line("ANALYZE_ACK|ERRO|FORMATO_INVALIDO")
                return

            try:
                expected_count = int(header_parts[2])
            except ValueError:
                self.write_line("ANALYZE_ACK|ERRO|FORMATO_INVALIDO")
                return

            if expected_count < 0:
                self.write_line("ANALYZE_ACK|ERRO|FORMATO_INVALIDO")
                return

            gateway_id = header_parts[1].strip()
            measurements: list[Measurement] = []
            terminator_received = False

            while True:
                line = read_protocol_line(self.rfile.readline())
                if not line:
                    break

                if line.upper() == "END":
                    terminator_received = True
                    break

                measurement = parse_measurement(line)
                if measurement is not None:
                    measurements.append(measurement)

            if not terminator_received or len(measurements) != expected_count:
                self.write_line("ANALYZE_ACK|ERRO|CONTAGEM_INVALIDA")
                return

            result = analyze(gateway_id, measurements)
            self.write_line(
                "ANALYZE_ACK|SUCESSO|"
                f"{result.gateway_id}|{result.generated_at}|{result.total_records}|"
                f"{result.average_value}|{result.max_value}|{result.risk_level}|{result.summary}"
            )

            print(
                f"[RPC ANALISE] {gateway_id}: {result.total_records} registos, "
                f"media={result.average_value}, risco={result.risk_level}",
                flush=True,
            )
        except Exception as exc:
            self.write_line(f"ANALYZE_ACK|ERRO|{exc}")

    def write_line(self, message: str) -> None:
        self.wfile.write(f"{message}\n".encode("utf-8"))
        self.wfile.flush()


def main() -> None:
    with ThreadingTcpServer(("0.0.0.0", PORT), AnalysisHandler) as server:
        print(f"[RPC ANALISE] Servico de analise ativo na porta {PORT}.", flush=True)
        print("[RPC ANALISE] Formato: ANALYZE_BATCH|gateway_id|n_registos ... END", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
