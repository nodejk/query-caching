package utils;

import cache.dim.Dimension;
import cache.enums.CacheType;
import cache.enums.DimensionType;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.gson.annotations.SerializedName;
import enums.Mode;
import enums.QueryType;
import org.apache.commons.io.FileUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class Configuration {

    private static final List<Integer> CACHE_SIZES =
            List.of(4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096);

    public static final Integer MINIMUM_DERIVABILITY_PERCENTAGE = 40;

    public static final List<Integer> DERIVABILITIES_PERCENTAGES =
            List.of(0, 10, 20, 25, 35, MINIMUM_DERIVABILITY_PERCENTAGE, 45, 60, 75, 78, 83, 88, 90);

    @SerializedName("cache_type")
    public CacheType cacheType;

    @SerializedName("mode")
    public Mode mode;

    @SerializedName("cache_size")
    public Integer cacheSize;

    @SerializedName("derivability")
    public Integer derivability;

    @SerializedName("query_type")
    public QueryType queryType;

    @SerializedName("lower_derivability")
    public Integer lowerDerivability;

    @SerializedName("dimension_type")
    public DimensionType dimensionType;

    public void validate() {
        if (!DERIVABILITIES_PERCENTAGES.contains(this.derivability)) {
            throw new Error("Wrong derivability given!");
        }

        if (!CACHE_SIZES.contains(this.cacheSize)) {
            throw new Error("Wrong cache size given!");
        }
    }

//
    public Configuration(
            String cacheType,
            String mode,
            Integer cacheSize,
            String queryType,
            Integer derivibility,
            String dimensionType
    ) {
        this.cacheSize = cacheSize;
        this.mode = Mode.fromString(mode);
        this.queryType = QueryType.fromString(queryType);
        this.cacheType = CacheType.fromString(cacheType);
        this.derivability = derivibility;
        this.dimensionType = DimensionType.fromString(dimensionType);

        this.setDerivability();
        this.validate();
    }

    public Dimension getDimension() {
        switch (this.dimensionType) {
            case SIZE_BYTES: {
                return Dimension.SIZE(this.cacheSize * FileUtils.ONE_MB);
            }
            case COUNT: {
                return Dimension.COUNT(this.cacheSize);
            }
            default: {
                throw new Error("wrong dimension provided");
            }
        }
    }

    public void setDerivability() {
        this.lowerDerivability = 0;

        if (this.derivability > 40) {
            return;
        }

        switch (this.derivability) {
            case 0: {
                this.lowerDerivability = 100;
                break;
            }
            case 10: {
                this.lowerDerivability = 94;
                break;
            }
            case 20: {
                this.lowerDerivability = 85;
                break;
            }
            case 30: {
                this.lowerDerivability = 80;
                break;
            }
            default: {
                this.lowerDerivability = 50;
                break;
            }
        }

        this.derivability = MINIMUM_DERIVABILITY_PERCENTAGE;
    }

    public void saveJsonConfiguration(String path) throws IOException {
        String jsonOutput;

        ObjectMapper mapper = new ObjectMapper();
        try {
            jsonOutput = mapper.writeValueAsString(this);

            FileWriter file = new FileWriter(path);

            file.write(jsonOutput);
            file.close();
        }
        catch (JsonGenerationException | JsonMappingException e) {
            e.printStackTrace();
        }
    }
}
