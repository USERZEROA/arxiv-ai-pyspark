# Spot check of the AI rules

`reports/spot_check_sample.csv` holds 36 records from the study window (six per
group, picked by a hash of the id, so the same records come back on every run).
I read each title, its categories and the matched phrase in context. This is a
sanity check of the rules, not an accuracy estimate - 36 records are far too few
for that, and no accuracy number is claimed anywhere in the project.

| Group | Records | Reading |
|---|---:|---|
| AI: category + text | 6 | All six are clearly AI/ML papers (deep learning framework, PLM calibration, federated learning, RL meta-agents, LLM agents). |
| AI: category only | 6 | All six are ML, vision, robotics or speech papers whose abstracts use words the rules do not list (convolutions, autoencoders, VLA models, ASR). Shows why the category flag is needed. |
| AI: text only | 6 | Four are AI-related (AI-generated images, vision-language retrieval, deep-learning control of wind turbines, AI in higher education). Two are weak: `2212.07682` (submodular ranking) only says the problem has "applications in machine learning", and `2602.01883` (saddle dynamics) uses "neural-network loss landscapes" as a test case. |
| AI: GenAI only through the `LLM` acronym | 6 | Five use LLMs as a method or subject (music foundation model, code red-teaming agents, RAG layout repair, vulnerability input generation, query compilation). `2601.15139` (PyPI metadata survey) is borderline: LLMs are only used for topic modelling of survey answers. |
| Not AI, contains transformer/diffusion/agent/attention | 6 | Five are correctly left out (exciton diffusion, evaporative lithography, AR pointing, heat conduction, qubits). `2606.28125` ("Humans, Bots, and Agents ... in Pull Requests") is about coding agents and is probably a missed AI paper - the price of not matching the bare word "agent". |
| Not AI, other | 6 | All six are correctly non-AI (astrophysics, signal processing, cryptoeconomics, particle physics, statistical mechanics, quantum magic). |

Takeaways for the write-up:

* Text matches pull in some papers that only mention ML in passing, so the
  text-only part of `ai_related` is an upper-leaning count.
* Leaving out "agent", "transformer" and "diffusion" avoids many false hits in
  physics and economics but misses some recent agent papers.
* The GenAI/LLM flag means "the abstract talks about LLMs/GenAI", not "the
  authors used an LLM".
