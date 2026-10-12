# Decisão local com LitJev (SDD 3a.2b, CA6)

Roteiro **manual**, fora do CI: roda o exemplo `stint-example-ai-worker` contra um servidor LitJev real nesta máquina e mede acertos, confianças, latência e memória com 15 reuniões sintéticas em pt-BR. O build nunca depende disto. O `mvn verify` usa um servidor falso em porta aleatória e nunca chega a `127.0.0.1:8000`.

## O que está fixado

| Item | Valor |
|---|---|
| LitJev | `github.com/zhengxuyu/litjev`, commit `2cd915aff989b37bb1c46ef5ae480c4a01501404` (01/10/2026), Apache-2.0. Independente, sem afiliação com a TypeSafe AI |
| Modelo | `Qwen/Qwen3.5-2B`, revisão `15852e8c16360a2fea060d615a32b45270f8a8fc`, **Apache-2.0** (card do Hugging Face, conferido em 12/10/2026), cerca de 4,6 GB em 16 bits |
| Plano B | `Qwen/Qwen3.5-0.8B`, revisão `2fc06364715b967f1860aea9cf38778875588b17`, Apache-2.0, cerca de 1,75 GB |
| Leitura | `content` (padrão do LitJev), backend transformers |
| Precisão | `float16`: a RTX 2060 SUPER (Turing) não tem BF16 nativo |
| Reuniões | `docs/manual/local-decision/meetings/*.json`: 5 ata, 5 adr, 5 runbook; 7 urgentes e 8 não. Sintéticas, sem pessoa, empresa ou reunião real. O rótulo fica no arquivo e não vai para o modelo |

## Por que no WSL2
O `pyproject.toml` do LitJev só aponta as rodas do PyTorch com CUDA 12.8 para Linux (`marker = "sys_platform == 'linux'"`). No Windows, o `uv run --locked` instala o torch do PyPI, que roda só na CPU. No WSL2 Ubuntu a GPU fica disponível pelo driver NVIDIA do Windows, e o WSL2 encaminha `127.0.0.1:8000` para o Windows, onde roda o exemplo.

## 1. Preparar o WSL2 (uma vez)
O C: tem pouco espaço livre, então os caches vão para o E:.

```bash
# no Ubuntu do WSL2
nvidia-smi                                   # a GPU precisa aparecer aqui
curl -LsSf https://astral.sh/uv/install.sh | sh
mkdir -p /mnt/e/stint-local-decision/{hf,uv}
export HF_HOME=/mnt/e/stint-local-decision/hf
export UV_CACHE_DIR=/mnt/e/stint-local-decision/uv
git clone https://github.com/zhengxuyu/litjev.git ~/litjev
cd ~/litjev && git checkout 2cd915aff989b37bb1c46ef5ae480c4a01501404
uv sync --locked --python 3.13
```

## 2. Subir o LitJev
```bash
cd ~/litjev
HF_HOME=/mnt/e/stint-local-decision/hf UV_CACHE_DIR=/mnt/e/stint-local-decision/uv \
  uv run --locked --python 3.13 litjev --model Qwen/Qwen3.5-2B \
  --revision 15852e8c16360a2fea060d615a32b45270f8a8fc --dtype float16 --port 8000
```

`--python 3.13` é necessário: o `requires-python >=3.11` do LitJev deixa o uv escolher o Python mais novo (3.15), para o qual o `torch==2.11.0+cu128` travado no `uv.lock` não tem roda.

O LitJev sobe só em `127.0.0.1` e não tem autenticação. **Não o exponha na rede.**

Sem memória de GPU suficiente, troque para o plano B (`Qwen/Qwen3.5-0.8B` e a revisão dele). Sem GPU, use `--device-map cpu --dtype float32`, que é bem mais lento.

## 3. Aquecer
O primeiro pedido baixa e carrega o modelo, e pode levar minutos. Aqueça antes de medir:

```bash
curl --fail-with-body http://127.0.0.1:8000/v1/systemone -H 'Content-Type: application/json' \
  -d '{"model":"litjev","state":"aquecimento","questions":{"q":{"type":"noul","instructions":"Isto é um teste?"}}}'
curl http://127.0.0.1:8000/health          # {"status":"ok","model_loaded":true}
```

## 4. Rodar o exemplo (no Windows, Git Bash)
```bash
cd /c/dev/CDO
# o jar do exemplo (mvn install já o gera em target/quarkus-app)
java -Dstint.example.litjev.base-uri=http://127.0.0.1:8000 \
     -Dstint.example.litjev.model=Qwen/Qwen3.5-2B \
     -Dstint.example.litjev.server-version=2cd915a \
     -Dstint.example.litjev.readout=content \
     -jar examples/stint-example-ai-worker/target/quarkus-app/quarkus-run.jar \
     triage docs/manual/local-decision/meetings | grep '^TRIAGE'
```

O modo `triage` roda o workflow `meeting-triage` (as duas decisões do exemplo) para cada reunião, com as duas rotas no adapter Jev em perfil local (`server(LITJEV)`). O adapter recusa um `base-uri` que não seja de loopback.

A saída tem uma linha por reunião:
- o rótulo esperado e o valor decidido, com `MISS` quando erra;
- a confiança pela maior probabilidade, que é a fonte padrão do perfil local, e a Gini do LitJev;
- se a decisão ficou `certain` ou `uncertain` com o limiar padrão de 0,7;
- a latência da chamada.

No fim vêm os acertos, a distribuição das confianças (por acerto e erro) e a latência por decisão (mínimo, mediana, máximo e p95).

## 5. Medir a memória
Durante a execução, em outro terminal do WSL2:

```bash
nvidia-smi --query-gpu=memory.used,memory.total --format=csv -l 1   # pico de VRAM
ps -o rss= -p "$(pgrep -f 'litjev --model')"                         # RAM do processo, em KB
```

## 6. Registrar
Na seção 9 do `docs/sdd/3a.2b.md`, CA6:
- acertos de documento e de urgência;
- confianças (as linhas `TRIAGE … confidence`);
- latência por decisão (mediana e p95);
- pico de VRAM e RAM;
- hardware (CPU, GPU, RAM);
- modelo, revisão e licença;
- versão do LitJev e leitura.

Os números valem só para este modelo, este prompt do LitJev e estas reuniões. O LitJev não vem calibrado, e o limiar do adapter `litjev` deve ser ajustado com dados reais (3a.4). Ele nunca é copiado do Jev hospedado.
