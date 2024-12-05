# Set version
export VERSION=${GITHUB_REF_NAME#"v"}
ls 
sed -i -E "s/^(version =.*)/version = \"$VERSION\"/g" pyproject.toml
echo -e "Getting tag to publish:\n    $(cat pyproject.toml | grep "version =")"

