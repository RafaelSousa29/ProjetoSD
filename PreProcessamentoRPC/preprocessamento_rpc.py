from __future__ import annotations

from dataclasses import dataclass
import math
import socketserver


PORT = 7000


@dataclass(frozen=True)
class PreprocessResult:
    success: bool
    value: str = ""
    error: str = ""


class ThreadingTcpServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


def read_protocol_line(raw_line: bytes) -> str:
    return raw_line.decode("utf-8-sig").strip()


def format_number(value: float) -> str:
    text = f"{value:.2f}".rstrip("0").rstrip(".")
    return "0" if text == "-0" else text


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def extract_numeric_value(data_type: str, raw_value: str) -> PreprocessResult:
    raw_input = raw_value.strip()
    if not raw_input:
        return PreprocessResult(False, error="VALOR_VAZIO")

    numeric_text = raw_input
    if raw_input.upper().startswith(data_type):
        remaining_input = raw_input[len(data_type):].strip()
        if len(remaining_input) < 2:
            return PreprocessResult(False, error="FORMATO_VALOR_INVALIDO")

        separator = remaining_input[0]
        if separator not in ("=", "-", ".", "/"):
            return PreprocessResult(False, error="SEPARADOR_INVALIDO")

        numeric_text = remaining_input[1:].strip()

    normalized = numeric_text.replace(",", ".").strip()

    try:
        numeric_value = float(normalized)
    except ValueError:
        return PreprocessResult(False, error="VALOR_NAO_NUMERICO")

    if not math.isfinite(numeric_value):
        return PreprocessResult(False, error="VALOR_NAO_NUMERICO")

    return PreprocessResult(True, value=str(numeric_value))


def preprocess_value(data_type: str, raw_value: str) -> PreprocessResult:
    extracted = extract_numeric_value(data_type, raw_value)
    if not extracted.success:
        return extracted

    numeric_value = float(extracted.value)

    if data_type == "TEMP":
        numeric_value = clamp(numeric_value, -30, 70)
    elif data_type == "HUM":
        numeric_value = clamp(numeric_value, 0, 100)
    elif data_type == "RUIDO":
        numeric_value = clamp(numeric_value, 0, 140)
    elif data_type in ("PM2.5", "PM10", "LUM"):
        numeric_value = max(0, numeric_value)
    elif data_type == "AQ":
        numeric_value = clamp(numeric_value, 0, 500)

    return PreprocessResult(True, value=format_number(numeric_value))


class PreprocessHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        try:
            line = read_protocol_line(self.rfile.readline())
            if not line:
                return

            parts = line.split("|")
            if len(parts) < 6 or parts[0].upper() != "PREPROCESS":
                self.write_line("PREPROCESS_ACK|ERRO|FORMATO_INVALIDO")
                return

            gateway_id = parts[1].strip()
            sensor_id = parts[2].strip()
            timestamp = parts[3].strip()
            data_type = parts[4].strip().upper()
            raw_value = parts[5].strip()

            result = preprocess_value(data_type, raw_value)
            if not result.success:
                self.write_line(f"PREPROCESS_ACK|ERRO|{result.error}")
                print(f"[RPC PRE] Rejeitado {sensor_id}/{data_type}: {result.error}", flush=True)
                return

            self.write_line(
                f"PREPROCESS_ACK|SUCESSO|{sensor_id}|{timestamp}|{data_type}|{result.value}"
            )
            print(
                f"[RPC PRE] {gateway_id} pediu {sensor_id}/{data_type}: {raw_value} -> {result.value}",
                flush=True,
            )
        except Exception as exc:
            self.write_line(f"PREPROCESS_ACK|ERRO|{exc}")

    def write_line(self, message: str) -> None:
        self.wfile.write(f"{message}\n".encode("utf-8"))
        self.wfile.flush()


def main() -> None:
    with ThreadingTcpServer(("0.0.0.0", PORT), PreprocessHandler) as server:
        print(f"[RPC PRE] Servico de pre-processamento ativo na porta {PORT}.", flush=True)
        print("[RPC PRE] Formato: PREPROCESS|gateway_id|sensor_id|timestamp|tipo|valor", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
