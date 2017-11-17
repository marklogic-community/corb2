<?xml version="1.0"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="text" encoding="UTF-8" />

    <xsl:template match="*[name(*[1]) = name(*[1]/following-sibling::*)]">
        <xsl:text>{</xsl:text>
        <xsl:apply-templates select="." mode="property-name"/>
        <xsl:text>[</xsl:text>
        <xsl:apply-templates select="*" mode="named-object"/>
        <xsl:text>]</xsl:text>
        <xsl:text>}</xsl:text>
    </xsl:template>

    <xsl:template match="*">
        <xsl:apply-templates select="." mode="named-object"/>
    </xsl:template>

    <xsl:template match="@* | *" mode="array-value">
        <xsl:apply-templates select="." mode="object"/>
        <xsl:if test="not(position() = last())">
            <xsl:text>,</xsl:text>
        </xsl:if>
    </xsl:template>

    <xsl:template match="*" mode="named-object">
        <xsl:text>{</xsl:text>
        <xsl:apply-templates select="." mode="property-name"/>
        <xsl:text>{</xsl:text>
        <xsl:apply-templates select="@*|*|text()[normalize-space()]" mode="property"/>
        <xsl:text>}}</xsl:text>
        <xsl:if test="not(position() = last())">
            <xsl:text>,</xsl:text>
        </xsl:if>
    </xsl:template>

    <xsl:template match="*" mode="object">
        <xsl:text>{</xsl:text>
        <xsl:apply-templates select="@*|*|text()[normalize-space()]" mode="property"/>
        <xsl:text>}</xsl:text>
    </xsl:template>

    <xsl:template match="*" mode="array">
        <xsl:text>[</xsl:text>
        <xsl:apply-templates select="@*|*|text()[normalize-space()]" mode="array-value"/>
        <xsl:text>]</xsl:text>
    </xsl:template>

    <xsl:template match="text()" mode="property-name">
        <xsl:apply-templates select=".." mode="property-name"/>
    </xsl:template>

    <xsl:template match="@* | * | text()[normalize-space()]" mode="property">

        <xsl:apply-templates select="." mode="property-name"/>

        <xsl:choose>
            <xsl:when test="*[2] and name(*[1]) = name(*[1]/following-sibling::*)">
                <xsl:apply-templates select="." mode="array"/>
            </xsl:when>
            <xsl:when test="*">
                <xsl:apply-templates select="." mode="object"/>
            </xsl:when>
            <xsl:when test="string-length(.) = 0">
                <xsl:text>null</xsl:text>
            </xsl:when>
            <xsl:otherwise>
                <xsl:variable name="wrap">
                    <xsl:if test="string(number(.)) = 'NaN' and not(. = 'true' or . = 'false')">
                        <xsl:text>"</xsl:text>
                    </xsl:if>
                </xsl:variable>
                <xsl:variable name="escapedValue">
                    <xsl:call-template name="escapeQuote"/>
                </xsl:variable>
                <xsl:value-of select="concat($wrap, $escapedValue, $wrap)"/>
            </xsl:otherwise>
        </xsl:choose>

        <xsl:if test="not(position() = last())">
            <xsl:text>,</xsl:text>
        </xsl:if>
    </xsl:template>

    <xsl:template match="@* | *" mode="property-name">
        <xsl:value-of select="concat('&quot;', local-name(), '&quot;', ': ')"/>
    </xsl:template>

    <xsl:template name="escapeQuote">
        <xsl:param name="value" select="."/>

        <xsl:if test="string-length($value) > 0">
            <xsl:value-of select="substring-before(concat($value, '&quot;'), '&quot;')"/>
            <xsl:if test="contains($value, '&quot;')">
                <xsl:text>\"</xsl:text>
                <xsl:call-template name="escapeQuote">
                    <xsl:with-param name="value" select="substring-after($value, '&quot;')"/>
                </xsl:call-template>
            </xsl:if>
        </xsl:if>
    </xsl:template>

</xsl:stylesheet>
