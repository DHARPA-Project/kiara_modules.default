# -*- coding: utf-8 -*-
import gensim
import nltk
import pandas as pd
import pyarrow as pa
import re
import typing
from gensim import corpora
from gensim.models import CoherenceModel
from pyarrow import Table
from pydantic import Field
from spacy.tokens import Doc
from spacy.util import DummyTokenizer

from kiara import KiaraModule
from kiara.data.values import ValueSchema, ValueSet
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import KiaraModuleConfig


def get_stopwords():

    # TODO: make that smarter
    pass

    # nltk.download('punkt')
    # nltk.download('stopwords')
    from nltk.corpus import stopwords

    return stopwords


class TokenizeTextConfig(KiaraModuleConfig):

    filter_non_alpha: bool = Field(
        description="Whether to filter out non alpha tokens.", default=True
    )
    min_token_length: int = Field(description="The minimum token length.", default=3)
    to_lowercase: bool = Field(
        description="Whether to lowercase the tokens.", default=True
    )


class TokenizeTextModule(KiaraModule):

    _config_cls = TokenizeTextConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs = {"text": {"type": "string", "doc": "The text to tokenize."}}

        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs = {
            "token_list": {
                "type": "list",
                "doc": "The tokenized version of the input text.",
            }
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        # TODO: module-independent caching?

        # language = inputs.get_value_data("language")
        #
        text = inputs.get_value_data("text")
        tokenized = nltk.word_tokenize(text)

        result = tokenized
        if self.get_config_value("min_token_length") > 0:
            result = (
                x
                for x in tokenized
                if len(x) >= self.get_config_value("min_token_length")
            )

        if self.get_config_value("filter_non_alpha"):
            result = (x for x in result if x.isalpha())

        if self.get_config_value("to_lowercase"):
            result = (x.lower() for x in result)

        outputs.set_value("token_list", list(result))


class RemoveStopwordsModule(KiaraModule):
    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        # TODO: do something smart and check whether languages are already downloaded, if so, display selection in doc
        inputs: typing.Dict[str, typing.Dict[str, typing.Any]] = {
            "token_lists": {
                "type": "array",
                "doc": "An array of string lists (a list of tokens).",
            },
            "languages": {
                "type": "list",
                # "doc": f"A list of language names to use default stopword lists for. Available: {', '.join(get_stopwords().fileids())}.",
                "doc": "A list of language names to use default stopword lists for.",
                "optional": True,
            },
            "additional_stopwords": {
                "type": "list",
                "doc": "A list of additional, custom stopwords.",
                "optional": True,
            },
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        outputs = {
            "token_list": {
                "type": "array",
                "doc": "An array of string lists, with the stopwords removed.",
            }
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        custom_stopwords = inputs.get_value_data("additional_stopwords")
        languages = inputs.get_value_data("languages")
        if isinstance(languages, str):
            languages = [languages]

        stopwords = set()
        if languages:
            for language in languages:
                if language not in get_stopwords().fileids():
                    raise KiaraProcessingException(
                        f"Invalid language: {language}. Available: {', '.join(get_stopwords().fileids())}."
                    )
                stopwords.update(get_stopwords().words(language))

        if custom_stopwords:
            stopwords.update(custom_stopwords)

        if not stopwords:
            outputs.set_value("token_list", inputs.get_value_obj("token_lists"))
            return

        token_lists = inputs.get_value_data("token_lists")

        if hasattr(token_lists, "to_pylist"):
            token_lists = token_lists.to_pylist()

        result = []
        for token_list in token_lists:

            cleaned_list = [x for x in token_list if x not in stopwords]
            result.append(cleaned_list)

        outputs.set_value("token_list", pa.array(result))


class LemmatizeTokensModule(KiaraModule):
    """Lemmatize a single token list."""

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        inputs = {"tokens_array": {"type": "list", "doc": "A list of tokens."}}
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        outputs = {
            "tokens_array": {"type": "list", "doc": "A list of lemmatized tokens."}
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        tokens = inputs.get_value_data("tokens_array")
        print(f"LEMMA: {tokens[0: 20]}")

        # TODO: install this on demand?
        import it_core_news_sm

        it_nlp = it_core_news_sm.load(disable=["tagger", "parser", "ner"])

        lemmatized_doc = []
        for w in tokens:
            w_lemma = [token.lemma_ for token in it_nlp(w)]
            lemmatized_doc.append(w_lemma[0])

        outputs.set_value("tokens_array", lemmatized_doc)


class LemmatizeTokensArrayModule(KiaraModule):
    """Lemmatize an array of token lists.

    Compared to using the ``lemmatize_tokens`` module in combination with ``map``, this is much faster, since it uses
    a spacy [pipe](https://spacy.io/api/language#pipe) under the hood.
    """

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        inputs = {
            "tokens_array": {"type": "array", "doc": "An array of lists of tokens."}
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        outputs = {
            "tokens_array": {
                "type": "array",
                "doc": "An array of lists of lemmatized tokens.",
            }
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        tokens: pa.Array = inputs.get_value_data("tokens_array")

        # TODO: install this on demand?
        import it_core_news_sm

        it_nlp = it_core_news_sm.load(disable=["tagger", "parser", "ner"])

        class CustomTokenizer(DummyTokenizer):
            def __init__(self, vocab):
                self.vocab = vocab

            def __call__(self, words):
                return Doc(self.vocab, words=words)

        it_nlp.tokenizer = CustomTokenizer(it_nlp.vocab)
        result = []

        for doc in it_nlp.pipe(
            tokens.to_pylist(),
            batch_size=32,
            n_process=3,
            disable=["parser", "ner", "tagger"],
        ):
            result.append([tok.lemma_ for tok in doc])

        outputs.set_value("tokens_array", pa.array(result))


class LDAModule(KiaraModule):
    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        inputs: typing.Dict[str, typing.Dict[str, typing.Any]] = {
            "tokens_array": {"type": "array", "doc": "The text corpus."},
            "num_topics": {
                "type": "integer",
                "doc": "The number of topics.",
                "default": 7,
            },
            "compute_coherence": {
                "type": "boolean",
                "doc": "Whether to train the model without coherence calculation.",
                "default": False,
            },
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs = {
            "topic_model": {
                "type": "table",
                "doc": "A table with 'topic_id' and 'words' columns (also 'num_topics', if coherence calculation was switched on).",
            }
        }
        return outputs

    def compute_with_coherence(self, corpus, id2word, corpus_model):

        topics_nr = []
        coherence_values_gensim = []
        models = []
        models_idx = [x for x in range(3, 20)]
        for num_topics in range(3, 20):
            # fastest processing time preset (hypothetically less accurate)
            model = gensim.models.ldamulticore.LdaMulticore(
                corpus, id2word=id2word, num_topics=num_topics, eval_every=None
            )
            # slower processing time preset (hypothetically more accurate) approx 20min for 700 short docs
            # model = gensim.models.ldamulticore.LdaMulticore(corpus, id2word=id2word, num_topics=num_topics, chunksize=1000, iterations = 200, passes = 10, eval_every = None)
            # slowest processing time preset approx 35min for 700 short docs (hypothetically even more accurate)
            # model = gensim.models.ldamulticore.LdaMulticore(corpus, id2word=id2word, num_topics=num_topics, chunksize=2000, iterations = 400, passes = 20, eval_every = None)
            models.append(model)
            coherencemodel = CoherenceModel(
                model=model, texts=corpus_model, dictionary=id2word, coherence="c_v"
            )
            coherence_value = coherencemodel.get_coherence()
            coherence_values_gensim.append(coherence_value)
            topics_nr.append(str(num_topics))

        df_coherence = pd.DataFrame(topics_nr, columns=["Number of topics"])
        df_coherence["Coherence"] = coherence_values_gensim

        # Create list with topics and topic words for each number of topics
        num_topics_list = []
        topics_list = []
        for i in range(len(models_idx)):
            numtopics = models_idx[i]
            num_topics_list.append(numtopics)
            model = models[i]
            topic_print = model.print_topics(num_words=30)
            topics_list.append(topic_print)

        df_coherence_table = pd.DataFrame(columns=["topic_id", "words", "num_topics"])

        idx = 0
        for i in range(len(topics_list)):
            for j in range(len(topics_list[i])):
                df_coherence_table.loc[idx] = ""
                df_coherence_table["topic_id"].loc[idx] = j + 1
                df_coherence_table["words"].loc[idx] = ", ".join(
                    re.findall(r'"(\w+)"', topics_list[i][j][1])
                )
                df_coherence_table["num_topics"].loc[idx] = num_topics_list[i]
                idx += 1

        return Table.from_pandas(df_coherence_table, preserve_index=False)

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        tokens_array = inputs.get_value_data("tokens_array")
        tokens = tokens_array.to_pylist()
        num_topics = inputs.get_value_data("num_topics")

        compute_coherence = inputs.get_value_data("compute_coherence")
        id2word = corpora.Dictionary(tokens)
        corpus = [id2word.doc2bow(text) for text in tokens]

        model = gensim.models.ldamulticore.LdaMulticore(
            corpus, id2word=id2word, num_topics=num_topics, eval_every=None
        )
        topic_print_model = model.print_topics(num_words=30)

        if not compute_coherence:
            df = pd.DataFrame(topic_print_model, columns=["topic_id", "words"])
            # TODO: create table directly
            result = Table.from_pandas(df)
        else:
            result = self.compute_with_coherence(
                corpus=corpus, id2word=id2word, corpus_model=tokens
            )

        outputs.set_value("topic_model", result)
