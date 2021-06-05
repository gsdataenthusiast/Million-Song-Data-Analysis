# Million-Song-Data-Analysis

The Million Song
Dataset one of the largest dataset that contains the metadata and audio analysis for
one million songs. It is a freely-available collection of audio features and metadata for
a million contemporary popular music tracks as part of a project that has been initiated
by The Echo Nest and LabROSA.
The primary dataset contains the feature analysis and the metadata of million songs
including fields like song ID, track ID, artist ID, and 51 different fields like title, year,
audio properties like beat, rhythm, loudness, etc.,
The Million Song Dataset is also includes many other complementary datasets in
relation to them. Our research focus will be limited to “Taste Profile” Dataset and “Top
MAGD” dataset. The “Taste Profile” dataset comprises of implicit user activity data
sourced from undisclosed organisation. All songs in “Taste Profile” dataset is available
in Million Song Dataset so for analysis purpose it is ideal to join them for any
information on attributes and features of a song.
The MSD All Music Genre Dataset (MAGD) contains Genre information for the songs
sourced from Allmusic.com
The Taste Profile dataset contains real user-song play counts from undisclosed
organisations. All songs have been matched to identifiers in the main million song
dataset and can be joined with this dataset to retrieve additional song attributes. This
is an implicit feedback dataset as users interact with songs by playing them but do not
explicitly indicate a preference for the song. This is an implicit feedback dataset as
users interact with songs by playing them but do not explicitly indicate a preference
for the song.
Focus of this project is to do data processing as required for understanding
and analysis, build few classification model for different genre based on audio
similarity and finally to build a song recommender system that can recommend songs
to the users based on their previous interactions captured as part of Taste Profile
dataset.
The analysis uses spark to interact with MSD data stored in Hadoop Distributed File
System.
