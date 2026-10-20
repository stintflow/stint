# Pilha local do marco da Fase 3a (SDD 3a.6, CA2)

Roteiro da execução manual do `stint-example-product-meeting` com modelos reais, todos locais e em loopback:
- transcrição pelo Whisper (speaches);
- decisões pelo LitJev;
- ata pelo `llama-server` do llama.cpp;
- áudio sintético em inglês gerado pelo Kokoro.

**Só dados sintéticos e nenhuma credencial real.** Nenhum servidor fica exposto na rede.

Hardware de referência: RTX 2060 SUPER de 8 GB (Turing), Ryzen 5 3600, 32 GB de RAM, Windows 11 com WSL2 Ubuntu. O desktop do Windows já ocupa cerca de 2,8 GB da GPU, e o LitJev com Qwen3.5-2B ocupa cerca de 5 GB (3a.2b). Por isso:
- o **gerador roda em CPU** (estratégia 3 do SDD, 4.4);
- o **Whisper roda em CPU** (estratégia 1);
- só o LitJev usa a GPU.

## Portas e configuração
| Peça | Onde roda | Porta | Propriedade do exemplo |
|---|---|---|---|
| speaches (Whisper) | Docker, imagem `ghcr.io/speaches-ai/speaches:latest-cpu` | `127.0.0.1:8001` | `stint.example.whisper.base-uri`, `stint.example.whisper.model` (`Systran/faster-distil-whisper-small.en`) |
| LitJev | WSL2, uv | `127.0.0.1:8000` | `stint.example.litjev.base-uri`, `stint.example.litjev.model` (`Qwen/Qwen3.5-2B`) |
| `llama-server` | Docker, imagem `ghcr.io/ggml-org/llama.cpp:server` (CPU) | `127.0.0.1:8080` | `stint.example.llm.base-uri`, `stint.example.llm.model` |
| Áudio | WSL2, `scripts/generate-audio.py` | — | `stint.example.corpus-dir` |

Licenças:
- código: speaches MIT, llama.cpp MIT, kokoro-onnx MIT, LitJev Apache-2.0;
- modelos: Whisper e distil-whisper MIT, Qwen3-4B-Instruct-2507 Apache-2.0, Qwen3.5-2B Apache-2.0, Kokoro-82M Apache-2.0.

## 1. Gerar o áudio (WSL2)
```bash
export UV_CACHE_DIR=/mnt/e/stint-local-decision/uv
mkdir -p /mnt/e/stint-3a6/tts && cd /mnt/e/stint-3a6/tts
curl -sSLO https://github.com/thewh1teagle/kokoro-onnx/releases/download/model-files-v1.0/kokoro-v1.0.onnx
curl -sSLO https://github.com/thewh1teagle/kokoro-onnx/releases/download/model-files-v1.0/voices-v1.0.bin
uv venv --python 3.12 .venv && uv pip install --python .venv/bin/python kokoro-onnx==0.4.9 soundfile pyyaml numpy
cd /mnt/c/dev/CDO/examples/stint-example-product-meeting
KOKORO_HOME=/mnt/e/stint-3a6/tts /mnt/e/stint-3a6/tts/.venv/bin/python scripts/generate-audio.py corpus
```
O script confere cada roteiro contra o manifesto e grava `corpus/audio/*.wav` (ignorados pelo Git). O hash de cada áudio vai para o manifesto.

## 2. Subir o Whisper (Windows, Git Bash)
```bash
docker volume create stint-speaches-cache
docker run -d --name stint-speaches -p 127.0.0.1:8001:8000 -v stint-speaches-cache:/home/ubuntu/.cache/huggingface \
  ghcr.io/speaches-ai/speaches:latest-cpu
curl -X POST http://127.0.0.1:8001/v1/models/Systran/faster-distil-whisper-small.en   # baixa o modelo
```

## 3. Subir o LitJev (WSL2)
```bash
cd /mnt/e/stint-local-decision/litjev
HF_HOME=/mnt/e/stint-local-decision/hf UV_CACHE_DIR=/mnt/e/stint-local-decision/uv \
  uv run --locked --python 3.13 litjev --model Qwen/Qwen3.5-2B \
  --revision 15852e8c16360a2fea060d615a32b45270f8a8fc --dtype float16 --port 8000
```
Aqueça como no `docs/manual/local-decision.md` (seção 3), de dentro do WSL.

## 4. Subir o gerador (Windows, Git Bash)
```bash
docker volume create stint-llama-cache
docker run -d --name stint-llama -p 127.0.0.1:8080:8080 -v stint-llama-cache:/root/.cache/llama.cpp \
  ghcr.io/ggml-org/llama.cpp:server -hf unsloth/Qwen3-4B-Instruct-2507-GGUF:Q4_K_M \
  --host 0.0.0.0 --port 8080 -c 8192 --threads 6 --alias qwen3-4b-instruct-2507-q4_k_m
```
O primeiro start baixa o GGUF (~2,5 GB).

## 5. Rodar o marco (Windows, Git Bash)
```bash
cd /c/dev/CDO
java -Dstint.example.corpus-dir=examples/stint-example-product-meeting/corpus \
  -Dstint.example.report-dir=target/product-meeting-report \
  -jar examples/stint-example-product-meeting/target/quarkus-app/quarkus-run.jar manual
```
- O relatório fica em `target/product-meeting-report/manual-<instante>.json` e `.md`.
- Ele traz latência por passo, tokens, custo (zero, tudo local), taxas de revisão e de `uncertain`, acerto contra o gabarito, WER e pico de memória da GPU, amostrado com `nvidia-smi`.
- No modo manual, o revisor roteirizado responde como diz o gabarito, como no CI. Revisões reais ficam para o portal do sandbox (S.5).

## 5b. Sem Docker (o caminho da execução de 15/10/2026)
Se o Docker Desktop não subir, os mesmos motores rodam direto no WSL2, ainda em loopback:
```bash
# Whisper: faster-whisper (o motor do speaches) atrás de um servidor mínimo compatível com a OpenAI
export UV_CACHE_DIR=/mnt/e/stint-local-decision/uv HF_HOME=/mnt/e/stint-3a6/hf
mkdir -p /mnt/e/stint-3a6/whisper && cd /mnt/e/stint-3a6/whisper
uv venv --python 3.12 .venv && uv pip install --python .venv/bin/python faster-whisper==1.1.1 requests
cd /mnt/c/dev/CDO/examples/stint-example-product-meeting
/mnt/e/stint-3a6/whisper/.venv/bin/python scripts/whisper-server.py --model Systran/faster-distil-whisper-small.en   --device cpu --threads 6 --port 8001

# Gerador: o binário de CPU do llama.cpp (release b11658) e o GGUF do Qwen3-4B-Instruct-2507
cd /mnt/e/stint-3a6/llama
curl -sSLO https://github.com/ggml-org/llama.cpp/releases/download/b11658/llama-b11658-bin-ubuntu-x64.tar.gz
tar xzf llama-b11658-bin-ubuntu-x64.tar.gz
curl -sSL -o Qwen3-4B-Instruct-2507-Q4_K_M.gguf   https://huggingface.co/unsloth/Qwen3-4B-Instruct-2507-GGUF/resolve/main/Qwen3-4B-Instruct-2507-Q4_K_M.gguf
# o binário pede libgomp.so.1: instale libgomp1 (apt), ou aponte LD_LIBRARY_PATH para uma cópia (a do torch do LitJev)
cd llama-b11658 && LD_LIBRARY_PATH=. ./llama-server -m ../Qwen3-4B-Instruct-2507-Q4_K_M.gguf --host 127.0.0.1   --port 8080 -c 8192 --threads 6 --alias qwen3-4b-instruct-2507-q4_k_m
```
O WSL2 encaminha `127.0.0.1` para o Windows, onde roda o exemplo.

## 6. Desligar
```bash
docker rm -f stint-speaches stint-llama        # e Ctrl+C no LitJev
```
