"""
GraphQL query documents for the Tokopedia gateway (gql.tokopedia.com).

These are data contracts, not configuration — they describe *what* fields the
crawler consumes and only change when the parsing logic changes. Endpoint
URLs, headers, and tunables live in :mod:`library.config`.

Queries were extracted from captured browser traffic and trimmed to the
fields the pipeline actually parses (ads/tracking subtrees removed).
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# SearchProductV5Query — keyword search for products
# ---------------------------------------------------------------------------

SEARCH_PRODUCT_QUERY = """\
query SearchProductV5Query($params: String!) {
  searchProductV5(params: $params) {
    header {
      totalData
      responseCode
      keywordProcess
      keywordIntention
      isQuerySafe
      additionalParams
      __typename
    }
    data {
      totalDataText
      redirection {
        url
        __typename
      }
      suggestion {
        currentKeyword
        suggestion
        query
        text
        __typename
      }
      ticker {
        oldID: id
        id: id_str_auto_
        text
        query
        applink
        __typename
      }
      violation {
        headerText
        descriptionText
        imageURL
        ctaURL
        ctaApplink
        buttonText
        buttonType
        __typename
      }
      products {
        oldID: id
        id: id_str_auto_
        ttsProductID
        name
        url
        applink
        mediaURL {
          image
          image300
          videoCustom
          __typename
        }
        shop {
          oldID: id
          id: id_str_auto_
          ttsSellerID
          name
          url
          city
          tier
          __typename
        }
        stock {
          ttsSKUID
          __typename
        }
        badge {
          oldID: id
          id: id_str_auto_
          title
          url
          __typename
        }
        price {
          text
          number
          range
          original
          discountPercentage
          __typename
        }
        freeShipping {
          url
          __typename
        }
        labelGroups {
          position
          title
          type
          url
          __typename
        }
        category {
          oldID: id
          id: id_str_auto_
          name
          breadcrumb
          __typename
        }
        rating
        wishlist
        meta {
          oldParentID: parentID
          parentID: parentID_str_auto_
          oldWarehouseID: warehouseID
          warehouseID: warehouseID_str_auto_
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
}
"""

# ---------------------------------------------------------------------------
# AceSearchShopQuery — keyword search for shops
# ---------------------------------------------------------------------------

SEARCH_SHOP_QUERY = """\
query AceSearchShopQuery($params: String!) {
  aceSearchShop(params: $params) {
    shops {
      old_shop_id: shop_id
      shop_id: shop_id_str_auto_
      shop_name
      shop_domain
      shop_location
      shop_status
      shop_tag_line
      shop_description
      reputation_score
      shop_total_favorite
      shop_gold_shop
      is_pm_pro
      is_official
      shop_url
      shop_image
      shop_lucky
      products {
        old_id: id
        id: id_str_auto_
        name
        url
        price
        image_url
        price_format
        __typename
      }
      favorited
      voucher {
        free_shipping
        cashback {
          cashback_value
          is_percentage
          __typename
        }
        __typename
      }
      __typename
    }
    suggestion {
      currentKeyword
      text
      query
      __typename
    }
    header {
      keyword_process
      response_code
      total_data
      __typename
    }
    __typename
  }
}
"""

# ---------------------------------------------------------------------------
# PDPMainInfo — product detail page main info
# ---------------------------------------------------------------------------

PRODUCT_DETAIL_QUERY = """\
fragment ProductMedia on pdpDataProductMedia {
  media {
    type
    urlOriginal: URLOriginal
    urlThumbnail: URLThumbnail
    urlMaxRes: URLMaxRes
    videoUrl: videoURLAndroid
    description
    __typename
  }
  __typename
}

fragment ProductHighlight on pdpDataProductContent {
  name
  price {
    value
    currency
    priceFmt
    slashPriceFmt
    discPercentage
    __typename
  }
  campaign {
    campaignID
    campaignType
    campaignTypeName
    percentageAmount
    originalPrice
    discountedPrice
    originalStock
    stock
    startDate
    endDate
    isActive
    __typename
  }
  stock {
    useStock
    value
    stockWording
    __typename
  }
  variant {
    isVariant
    parentID
    __typename
  }
  wholesale {
    minQty
    price {
      value
      currency
      __typename
    }
    __typename
  }
  isCashback {
    percentage
    __typename
  }
  isTradeIn
  isOS
  isPowerMerchant
  isWishlist
  isCOD
  preorder {
    duration
    timeUnit
    isActive
    preorderInDays
    __typename
  }
  __typename
}

fragment ProductDetail on pdpDataProductDetail {
  title
  productDetailDescription {
    title
    content
    __typename
  }
  content {
    title
    subtitle
    applink
    showAtFront
    isAnnotation
    __typename
  }
  __typename
}

fragment ProductSocial on pdpDataSocialProof {
  row
  content {
    icon
    title
    subtitle
    applink
    type
    rating
    __typename
  }
  __typename
}

query PDPMainInfo($productKey: String, $shopDomain: String, $layoutID: String, $extraPayload: String, $queryParam: String, $source: String, $userLocation: pdpUserLocation) {
  pdpMainInfo(shopDomain: $shopDomain, productKey: $productKey, layoutID: $layoutID, extraPayload: $extraPayload, queryParam: $queryParam, source: $source, userLocation: $userLocation) {
    requestID
    data {
      layoutName
      basicInfo {
        alias
        createdAt
        id: productID
        shopID
        shopName
        minOrder
        maxOrder
        weight
        weightUnit
        condition
        status
        url
        isTokoNow
        defaultMediaURL
        category {
          id
          name
          title
          breadcrumbURL
          isAdult
          detail {
            id
            name
            breadcrumbURL
            isAdult
            __typename
          }
          __typename
        }
        txStats {
          transactionSuccess
          transactionReject
          countSold
          paymentVerified
          itemSoldFmt
          __typename
        }
        stats {
          countView
          countReview
          countTalk
          rating
          __typename
        }
        productID
        __typename
      }
      __typename
    }
    components {
      name
      type
      position
      data {
        ...ProductMedia
        ...ProductHighlight
        ...ProductDetail
        ...ProductSocial
        __typename
      }
      __typename
    }
    __typename
  }
}
"""

# ---------------------------------------------------------------------------
# productReviewList — paginated product reviews
# ---------------------------------------------------------------------------

PRODUCT_REVIEWS_QUERY = """\
query productReviewList($productID: String!, $page: Int!, $limit: Int!, $sortBy: String, $filterBy: String) {
  productrevGetProductReviewList(productID: $productID, page: $page, limit: $limit, sortBy: $sortBy, filterBy: $filterBy) {
    productID
    list {
      id: feedbackID
      variantName
      message
      productRating
      reviewCreateTime
      reviewCreateTimestamp
      isAnonymous
      imageAttachments {
        attachmentID
        imageThumbnailUrl
        imageUrl
        __typename
      }
      videoAttachments {
        attachmentID
        videoUrl
        __typename
      }
      reviewResponse {
        message
        createTime
        __typename
      }
      user {
        userID
        fullName
        image
        url
        __typename
      }
      likeDislike {
        totalLike
        likeStatus
        __typename
      }
      badRatingReasonFmt
      __typename
    }
    shop {
      shopID
      name
      url
      image
      __typename
    }
    hasNext
    totalReviews
    __typename
  }
}
"""
