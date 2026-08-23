package com.llmate.multiprotocol.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LlmContent {
    private String type; // text, image
    private String text;
    private String base64Data;
    private String mimeType;

    public static LlmContent text(String text) {
        return LlmContent.builder().type("text").text(text).build();
    }

    public static LlmContent image(String base64Data, String mimeType) {
        return LlmContent.builder().type("image").base64Data(base64Data).mimeType(mimeType).build();
    }
}