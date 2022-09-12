# Dados dos Mananciais

<br>

Esse *script* tem como objetivo obter os dados sobre mananciais gerenciados pela [SABESP](http://site.sabesp.com.br), que abastecem a Região Metropolitana de São Paulo (RMSP). A gestão desses dados é feita pelo Laboratório de Sistemas de Suporte a Decisões em Engenharia Ambiental e de Recursos Hídricos ([LabSid](http://www.labsid.eng.br)), que dá suporte ao Sistema de Suporte a Decisões [SSD](http://ssd3sabesp.labsid.eng.br), bem como publiciza os dados no [*site* da SABESP](http://mananciais.sabesp.com.br/HistoricoSistemas).

<br>

O *script* foi desenvolvido a partir da interpretação do arquivo *json* que alimenta o *site*, identificado inspecionando a página. Funções específicas foram criadas para obter cada conjunto de dados (conforme o arquivo *json* se encontra separado). Ao final, para obtenção dos resultados, as funções são aplicadas em conjunto, otendo-se os dados para um intervalo entre datas, ou ainda, uma data única, com a finalidade de uma atualização diária.

<br>

-----

### *TODO*

1. Criar *script* para consumir esses dados >> http://mananciais.sabesp.com.br/api/Mananciais/ResumoTelemetricos/2020-04-01
2. O Presente código tem a finalidade de obter os dados unicamente do Sistema Cantareira e, portanto, não se pensou em adicionar a possibilidade de obter os dados dos outros sistemas produtores, por meio da inclusão e ajuste desse parâmetro definido e fixado como *0*.

<br>

-----

### Ajustes

Em 14.02.2022 fiz ajustes visto que funções não mais funcioonavam. A API alterou de "jaguari_cesp" para "jaguari_psb".
