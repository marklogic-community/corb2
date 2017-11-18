<?xml version="1.0"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:c="http://developer.marklogic.com/code/corb">
    <xsl:output method="text" encoding="UTF-8" />

    <c:escape-config>
        <c:replace from="\" to="\\"/>
        <c:replace from="&quot;" to="\&quot;"/>
        <c:replace from="&#xA;" to="\n"/>
        <c:replace from="&#xD;" to="\r"/>
        <c:replace from="&#x9;" to="\t"/>
        <c:replace from="\n" to="\n"/>
        <c:replace from="\r" to="\r"/>
        <c:replace from="\t" to="\t"/>
    </c:escape-config>

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
                    <xsl:if test="string(number(.)) = 'NaN' and not(. = 'true' or . = 'false' or . = 'null')">
                        <xsl:text>"</xsl:text>
                    </xsl:if>
                </xsl:variable>
                <xsl:variable name="escapedValue">
                    <xsl:call-template name="escape-string"/>
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
    
    <xsl:template name="escape-string">
        <xsl:param name="text" select="."/>
        <xsl:param name="escape-position" select="1"/>

        <xsl:variable name="escaped-value">
            <xsl:call-template name="replace-string">
                <xsl:with-param name="text" select="$text"/>
                <xsl:with-param name="replace" select="document('')/*/c:escape-config/c:replace[$escape-position]/@from"/>
                <xsl:with-param name="with" select="document('')/*/c:escape-config/c:replace[$escape-position]/@to"/>
            </xsl:call-template>
        </xsl:variable>

        <xsl:choose>
            <xsl:when test="$escape-position &lt; count(document('')/*/c:escape-config/c:replace)">
                <xsl:call-template name="escape-string">
                    <xsl:with-param name="text" select="$escaped-value"/>
                    <xsl:with-param name="escape-position" select="$escape-position + 1"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="$text"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template name="replace-string">
        <xsl:param name="text"/>
        <xsl:param name="replace"/>
        <xsl:param name="with"/>
        <xsl:choose>
            <xsl:when test="contains($text,$replace)">
                <xsl:value-of select="substring-before($text,$replace)"/>
                <xsl:value-of select="$with"/>
                <xsl:call-template name="replace-string">
                    <xsl:with-param name="text"
                                    select="substring-after($text,$replace)"/>
                    <xsl:with-param name="replace" select="$replace"/>
                    <xsl:with-param name="with" select="$with"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="$text"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

</xsl:stylesheet>
