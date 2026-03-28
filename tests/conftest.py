"""Shared fixtures for tests."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from src.config import Settings
from src.db.writer import MockAsyncDBWriter

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def settings() -> Settings:
    return Settings(
        db_host="localhost",
        db_port=5432,
        db_name="test_metadata",
        db_user="test",
        db_password="test",
        download_dir=Path("/tmp/mc_test/downloads"),
        cache_dir=Path("/tmp/mc_test/cache"),
        log_dir=Path("/tmp/mc_test/logs"),
    )


@pytest.fixture
def mock_writer() -> MockAsyncDBWriter:
    return MockAsyncDBWriter()


# ── Sample XML fixtures ──────────────────────────────────────

SAMPLE_SRA_XML = textwrap.dedent("""\
<?xml version="1.0" encoding="UTF-8"?>
<EXPERIMENT_PACKAGE_SET>
  <EXPERIMENT_PACKAGE>
    <EXPERIMENT accession="SRX000001" alias="exp1">
      <TITLE>RNA-Seq of human liver</TITLE>
      <STUDY_REF accession="SRP000001"/>
      <DESIGN>
        <SAMPLE_DESCRIPTOR accession="SRS000001"/>
        <LIBRARY_DESCRIPTOR>
          <LIBRARY_STRATEGY>RNA-Seq</LIBRARY_STRATEGY>
          <LIBRARY_SOURCE>TRANSCRIPTOMIC</LIBRARY_SOURCE>
          <LIBRARY_SELECTION>cDNA</LIBRARY_SELECTION>
          <LIBRARY_LAYOUT><PAIRED/></LIBRARY_LAYOUT>
        </LIBRARY_DESCRIPTOR>
      </DESIGN>
      <PLATFORM>
        <ILLUMINA>
          <INSTRUMENT_MODEL>Illumina HiSeq 2500</INSTRUMENT_MODEL>
        </ILLUMINA>
      </PLATFORM>
    </EXPERIMENT>
    <STUDY accession="SRP000001" alias="study1" center_name="GEO">
      <DESCRIPTOR>
        <STUDY_TITLE>Human Liver Transcriptome</STUDY_TITLE>
        <STUDY_ABSTRACT>A study of the liver transcriptome.</STUDY_ABSTRACT>
        <STUDY_TYPE existing_study_type="Transcriptome Analysis"/>
      </DESCRIPTOR>
      <STUDY_LINKS>
        <STUDY_LINK>
          <XREF_LINK>
            <DB>BioProject</DB>
            <ID>PRJNA000001</ID>
          </XREF_LINK>
        </STUDY_LINK>
      </STUDY_LINKS>
    </STUDY>
    <SAMPLE accession="SRS000001" alias="sample1">
      <TITLE>Human liver sample</TITLE>
      <SAMPLE_NAME>
        <TAXON_ID>9606</TAXON_ID>
        <SCIENTIFIC_NAME>Homo sapiens</SCIENTIFIC_NAME>
      </SAMPLE_NAME>
      <SAMPLE_ATTRIBUTES>
        <SAMPLE_ATTRIBUTE>
          <TAG>tissue</TAG>
          <VALUE>liver</VALUE>
        </SAMPLE_ATTRIBUTE>
        <SAMPLE_ATTRIBUTE>
          <TAG>sex</TAG>
          <VALUE>male</VALUE>
        </SAMPLE_ATTRIBUTE>
      </SAMPLE_ATTRIBUTES>
      <EXTERNAL_ID namespace="BioSample">SAMN00000001</EXTERNAL_ID>
    </SAMPLE>
    <RUN_SET>
      <RUN accession="SRR000001" alias="run1" total_spots="10000000" total_bases="2000000000" size="500000000">
        <EXPERIMENT_REF accession="SRX000001"/>
        <SRAFiles>
          <SRAFile url="https://sra-downloadb.be-md.ncbi.nlm.nih.gov/sos5/sra-pub-zq-11/SRR000/001/SRR000001.sralite.1" filename="SRR000001.sralite.1" size="500000000"/>
        </SRAFiles>
      </RUN>
    </RUN_SET>
  </EXPERIMENT_PACKAGE>
</EXPERIMENT_PACKAGE_SET>
""")

SAMPLE_MINIML_XML = textwrap.dedent("""\
<?xml version="1.0" encoding="UTF-8"?>
<MINiML xmlns="http://www.ncbi.nlm.nih.gov/geo/info/MINiML" version="1.0">
  <Series iid="GSE12345">
    <Title>Test Series Title</Title>
    <Summary>This is a test summary for the series.</Summary>
    <Overall-Design>Case-control design.</Overall-Design>
    <Type>Expression profiling by high throughput sequencing</Type>
    <Pubmed-ID>12345678</Pubmed-ID>
    <Contributor>
      <Person><First>John</First><Last>Doe</Last></Person>
    </Contributor>
    <Status>
      <Submission-Date>2024-01-15</Submission-Date>
      <Last-Update-Date>2024-06-01</Last-Update-Date>
      <Release-Date>2024-03-01</Release-Date>
    </Status>
    <Relation type="SRA" target="https://www.ncbi.nlm.nih.gov/sra?term=SRP111111"/>
    <Relation type="BioProject" target="https://www.ncbi.nlm.nih.gov/bioproject/PRJNA111111"/>
  </Series>
  <Sample iid="GSM100001">
    <Title>Sample 1 - Control</Title>
    <Type>SRA</Type>
    <Channel position="1">
      <Source>Blood</Source>
      <Organism taxid="9606">Homo sapiens</Organism>
      <Characteristics tag="tissue">blood</Characteristics>
      <Characteristics tag="disease state">normal</Characteristics>
      <Treatment-Protocol>No treatment</Treatment-Protocol>
      <Extract-Protocol>Total RNA extraction using TRIzol</Extract-Protocol>
      <Label>biotin</Label>
      <Molecule>total RNA</Molecule>
    </Channel>
    <Platform-Ref ref="GPL16791"/>
    <Series-Ref ref="GSE12345"/>
    <Relation type="SRA" target="https://www.ncbi.nlm.nih.gov/sra?term=SRX222222"/>
    <Relation type="BioSample" target="https://www.ncbi.nlm.nih.gov/biosample/SAMN222222"/>
  </Sample>
  <Platform iid="GPL16791">
    <Title>Illumina HiSeq 2500 (Homo sapiens)</Title>
    <Technology>high-throughput sequencing</Technology>
    <Distribution>virtual</Distribution>
    <Organism taxid="9606">Homo sapiens</Organism>
    <Manufacturer>Illumina</Manufacturer>
    <Manufacture-Protocol>See manufacturer website.</Manufacture-Protocol>
  </Platform>
</MINiML>
""")

SAMPLE_SOFT_TEXT = textwrap.dedent("""\
^SERIES = GSE99999
!Series_title = Soft Test Series
!Series_summary = A SOFT format test.
!Series_overall_design = Simple design
^SAMPLE = GSM200001
!Sample_title = SOFT Sample 1
!Sample_source_name_ch1 = Brain tissue
!Sample_organism_ch1 = Homo sapiens
!Sample_characteristics_ch1 = tissue: brain
!Sample_characteristics_ch1 = age: 45
^PLATFORM = GPL570
!Platform_title = Affymetrix HG-U133 Plus 2.0
!Platform_technology = in situ oligonucleotide
!Platform_manufacturer = Affymetrix
""")


@pytest.fixture
def sra_xml_path(tmp_path: Path) -> Path:
    p = tmp_path / "sra_test.xml"
    p.write_text(SAMPLE_SRA_XML)
    return p


@pytest.fixture
def miniml_xml_path(tmp_path: Path) -> Path:
    p = tmp_path / "miniml_test.xml"
    p.write_text(SAMPLE_MINIML_XML)
    return p


@pytest.fixture
def soft_file_path(tmp_path: Path) -> Path:
    p = tmp_path / "test.soft"
    p.write_text(SAMPLE_SOFT_TEXT)
    return p
