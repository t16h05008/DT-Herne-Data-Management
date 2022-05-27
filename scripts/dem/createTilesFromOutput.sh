echo "Building pyramids in ./output"
gdalbuildvrt ./output/dem50.vrt ./output/dem50/*.tiff
gdalbuildvrt ./output/dem25.vrt ./output/dem25/*.tiff
gdalbuildvrt ./output/dem10.vrt ./output/dem10/*.tiff
gdalbuildvrt ./output/dem1.vrt ./output/dem1/*.tiff

echo "Creating subdirectories for each resolution"
mkdir ./output/terrain50
mkdir ./output/terrain25
mkdir ./output/terrain10
mkdir ./output/terrain1

echo "Creating tiles for resolution 50"
ctb-tile -f Mesh -C -N -o ./output/terrain50 ./output/dem50.vrt
ctb-tile -f Mesh -C -N -l -o ./output/terrain50 ./output/dem50.vrt
echo "Creating tiles for resolution 25"
ctb-tile -f Mesh -C -N -o ./output/terrain25 ./output/dem25.vrt
ctb-tile -f Mesh -C -N -l -o ./output/terrain25 ./output/dem25.vrt
echo "Creating tiles for resolution 10"
ctb-tile -f Mesh -C -N -o ./output/terrain10 ./output/dem10.vrt
ctb-tile -f Mesh -C -N -l -o ./output/terrain10 ./output/dem10.vrt
echo "Creating tiles for resolution 1"
ctb-tile -f Mesh -C -N -o ./output/terrain1 ./output/dem1.vrt
ctb-tile -f Mesh -C -N -l -o ./output/terrain1 ./output/dem1.vrt

